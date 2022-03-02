/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.ingestion.stores.redis

import java.{sql, util}
import com.google.protobuf.Timestamp
import feast.ingestion.utils.TypeConversion
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.metrics.source.RedisSinkMetricSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * High-level writer to Redis. Relies on `Persistence` implementation for actual storage layout.
  * Here we define general flow:
  *
  * 1. Deduplicate rows within one batch (group by key and get only latest (by timestamp))
  * 2. Read last-stored timestamp from Redis
  * 3. Check if current timestamp is more recent than already saved one
  * 4. Save to storage if it's the case
  */
class RedisSinkRelation(override val sqlContext: SQLContext, config: SparkRedisConfig)
    extends BaseRelation
    with InsertableRelation
    with Serializable {

  import RedisSinkRelation._

  override def schema: StructType = ???

  val persistence: Persistence = new HashTypePersistence(config)

  val sparkConf: SparkConf = sqlContext.sparkContext.getConf

  def newJedisClient(endpoint: RedisEndpoint): Jedis = {
    val jedis = new Jedis(endpoint.host, endpoint.port)
    if (endpoint.password.nonEmpty) {
      jedis.auth(endpoint.password)
    }
    jedis
  }

  def checkIfInClusterMode(endpoint: RedisEndpoint): Boolean = {
    val jedis     = newJedisClient(endpoint)
    val isCluster = Try(jedis.clusterInfo()).isSuccess
    jedis.close()
    isCluster
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // repartition for deduplication
    val dataToStore =
      if (config.repartitionByEntity && data.rdd.getNumPartitions > 1)
        data
          .repartition(data.rdd.getNumPartitions, config.entityColumns.map(col): _*)
          .localCheckpoint()
      else data

    val endpoint = RedisEndpoint(
      host = sparkConf.get("spark.redis.host"),
      port = sparkConf.get("spark.redis.port").toInt,
      password = sparkConf.get("spark.redis.password", "")
    )

    val isClusterMode = checkIfInClusterMode(endpoint)

    dataToStore.foreachPartition { partition: Iterator[Row] =>
      java.security.Security.setProperty("networkaddress.cache.ttl", "3");
      java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");

      val pipelineProvider = if (isClusterMode) {
        ClusterPipelineProvider(endpoint)
      } else {
        SingleNodePipelineProvider(newJedisClient(endpoint))
      }

      // grouped iterator to only allocate memory for a portion of rows
      partition.grouped(config.iteratorGroupingSize).foreach { batch =>
        // group by key and keep only latest row per each key
        val rowsWithKey: Map[String, Row] =
          compactRowsToLatestTimestamp(batch.map(row => dataKeyId(row) -> row)).toMap

        val keys         = rowsWithKey.keysIterator.toList
        val readPipeline = pipelineProvider.pipeline()
        val readResponses =
          keys.map(key => persistence.get(readPipeline, key.getBytes()))
        readPipeline.close()
        val storedValues   = readResponses.map(_.get())
        val timestamps     = storedValues.map(persistence.storedTimestamp)
        val timestampByKey = keys.zip(timestamps).toMap
        val expiryTimestampByKey = keys
          .zip(storedValues)
          .map { case (key, storedValue) =>
            (key, newExpiryTimestamp(rowsWithKey(key), storedValue))
          }
          .toMap

        val writePipeline = pipelineProvider.pipeline()
        rowsWithKey.foreach { case (key, row) =>
          timestampByKey(key) match {
            case Some(t) if (t.after(row.getAs[java.sql.Timestamp](config.timestampColumn))) =>
              ()
            case _ =>
              if (metricSource.nonEmpty) {
                val lag = System.currentTimeMillis() - row
                  .getAs[java.sql.Timestamp](config.timestampColumn)
                  .getTime

                metricSource.get.METRIC_TOTAL_ROWS_INSERTED.inc()
                metricSource.get.METRIC_ROWS_LAG.update(lag)
              }
              persistence.save(
                writePipeline,
                key.getBytes(),
                row,
                expiryTimestampByKey(key)
              )
          }
        }
        writePipeline.close()
      }
      pipelineProvider.close()
    }
    dataToStore.unpersist()
  }

  private def compactRowsToLatestTimestamp(rows: Seq[(String, Row)]) = rows
    .groupBy(_._1)
    .values
    .map(_.maxBy(_._2.getAs[java.sql.Timestamp](config.timestampColumn).getTime))

  /**
    * Key is built from entities columns values with prefix of entities columns names.
    */
  private def dataKeyId(row: Row): String = {
    val types = row.schema.fields.map(f => (f.name, f.dataType)).toMap

    val sortedEntities = config.entityColumns.sorted.toSeq
    val entityValues = sortedEntities
      .map(entity => (row.getAs[Any](entity), types(entity)))
      .map { case (value, v_type) =>
        TypeConversion.sqlTypeToString(value, v_type)
      }
    DigestUtils.md5Hex(
      s"${config.projectName}#${sortedEntities.mkString("#")}:${entityValues.mkString("#")}"
    )
  }

  private lazy val metricSource: Option[RedisSinkMetricSource] = {
    MetricInitializationLock.synchronized {
      // RedisSinkMetricSource needs to be registered on executor and SparkEnv must already exist.
      // Which is problematic, since metrics system is initialized before SparkEnv set.
      // That's why I moved source registering here
      if (SparkEnv.get.metricsSystem.getSourcesByName(RedisSinkMetricSource.sourceName).isEmpty) {
        SparkEnv.get.metricsSystem.registerSource(new RedisSinkMetricSource)
      }
    }

    SparkEnv.get.metricsSystem.getSourcesByName(RedisSinkMetricSource.sourceName) match {
      case Seq(source: RedisSinkMetricSource) => Some(source)
      case _                                  => None
    }
  }

  private def newExpiryTimestamp(
      row: Row,
      value: util.Map[Array[Byte], Array[Byte]]
  ): Option[java.sql.Timestamp] = {
    val currentMaxExpiry: Option[Long] = value.asScala.toMap
      .map { case (key, value) =>
        (wrapByteArray(key), value)
      }
      .get(config.expiryPrefix.getBytes())
      .map(Timestamp.parseFrom(_).getSeconds * 1000)

    val rowExpiry: Option[Long] =
      if (config.maxAge > 0)
        Some(
          row
            .getAs[java.sql.Timestamp](config.timestampColumn)
            .getTime + config.maxAge * 1000
        )
      else None

    (currentMaxExpiry, rowExpiry) match {
      case (_, None)            => None
      case (None, Some(expiry)) => Some(new sql.Timestamp(expiry))
      case (Some(currentExpiry), Some(newExpiry)) =>
        Some(new sql.Timestamp(currentExpiry max newExpiry))
    }

  }
}

object RedisSinkRelation {
  object MetricInitializationLock
}
