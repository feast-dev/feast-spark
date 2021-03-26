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
package feast.ingestion

import feast.ingestion.metrics.IngestionPipelineMetrics
import feast.ingestion.sources.bq.BigQueryReader
import feast.ingestion.sources.file.FileReader
import feast.ingestion.validation.{RowValidator, TypeCheck}
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoder, Row, SaveMode, SparkSession}

/**
  * Batch Ingestion Flow:
  * 1. Read from source (BQ | File)
  * 2. Map source columns to FeatureTable's schema
  * 3. Validate
  * 4. Store valid rows in redis
  * 5. Store invalid rows in parquet format at `deadletter` destination
  */
object BatchPipeline extends BasePipeline {
  override def createPipeline(
      sparkSession: SparkSession,
      config: IngestionJobConfig
  ): Option[StreamingQuery] = {
    val featureTable = config.featureTable
    val projection =
      BasePipeline.inputProjection(config.source, featureTable.features, featureTable.entities)
    val rowValidator = new RowValidator(featureTable, config.source.eventTimestampColumn)
    val metrics      = new IngestionPipelineMetrics

    val input = config.source match {
      case source: BQSource =>
        BigQueryReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
      case source: FileSource =>
        FileReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
    }

    val projected = input.select(projection: _*).cache()

    TypeCheck.allTypesMatch(projected.schema, featureTable) match {
      case Some(error) =>
        throw new RuntimeException(s"Dataframe columns don't match expected feature types: $error")
      case _ => ()
    }

    implicit val rowEncoder: Encoder[Row] = RowEncoder(projected.schema)

    val validRows = projected
      .map(metrics.incrementRead)
      .filter(rowValidator.allChecks)

    config.store match {
      case _: RedisConfig =>
        validRows.write
          .format("feast.ingestion.stores.redis")
          .option("entity_columns", featureTable.entities.map(_.name).mkString(","))
          .option("namespace", featureTable.name)
          .option("project_name", featureTable.project)
          .option("timestamp_column", config.source.eventTimestampColumn)
          .option("max_age", config.featureTable.maxAge.getOrElse(0L))
          .save()
      case c: CassandraConfig =>
        val feastTypeToSpark = Map(
          "BYTES"       -> "BINARY",
          "STRING"      -> "STRING",
          "INT32"       -> "INT",
          "INT64"       -> "BIGINT",
          "DOUBLE"      -> "DOUBLE",
          "FLOAT"       -> "FLOAT",
          "BOOL"        -> "BOOLEAN",
          "BYTES_LIST"  -> "ARRAY<BINARY>",
          "STRING_LIST" -> "ARRAY<STRING>",
          "INT32_LIST"  -> "ARRAY<INT>",
          "INT64_LIST"  -> "ARRAY<BIGINT>",
          "DOUBLE_LIST" -> "ARRAY<DOUBLE>",
          "FLOAT_LIST"  -> "ARRAY<FLOAT>",
          "BOOL_LIST"   -> "ARRAY<BOOLEAN>"
        )

        def sanitizedForCassandra(expr: String) = expr.replace("-", "_")

        val tableName = sanitizedForCassandra(
          s"${featureTable.project}_${featureTable.entities.map(_.name).sorted.mkString("_")}"
        )
        val entityColumnType = featureTable.entities
          .map(e => s"${e.name} ${feastTypeToSpark(e.`type`.name())}")
          .map(sanitizedForCassandra)
        val featureColumnType = featureTable.features
          .map(f => s"${featureTable.name}_${f.name} ${feastTypeToSpark(f.`type`.name())}")
          .map(sanitizedForCassandra)

        sparkSession.sql(s"""
             |CREATE TABLE IF NOT EXISTS feast.${c.keyspace}.`${tableName}`
             |(${(entityColumnType ++ featureColumnType).mkString(", ")})
             |USING cassandra
             |PARTITIONED BY (${featureTable.entities
          .map(_.name)
          .map(sanitizedForCassandra)
          .mkString(", ")})
             |""".stripMargin)

        val prefixProjection = validRows.columns.map(c =>
          if (featureTable.features.map(_.name).contains(c))
            col(c).as(sanitizedForCassandra(s"${featureTable.name}_${c}"))
          else col(c).as(sanitizedForCassandra(c))
        )

        validRows
          .select(prefixProjection: _*)
          .writeTo(s"feast.${c.keyspace}.`${tableName}`")
          .option("writeTime", config.source.eventTimestampColumn)
          .append()

    }

    config.deadLetterPath foreach { path =>
      projected
        .filter(!rowValidator.allChecks)
        .map(metrics.incrementDeadLetters)
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .save(StringUtils.stripEnd(path, "/") + "/" + SparkEnv.get.conf.getAppId)
    }

    None
  }
}
