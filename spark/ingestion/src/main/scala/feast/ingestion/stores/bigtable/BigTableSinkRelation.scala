/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.ingestion.stores.bigtable

import com.google.cloud.bigtable.hbase.BigtableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{
  HColumnDescriptor,
  HTableDescriptor,
  TableExistsException,
  TableName
}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, length, struct, udf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StringType, StructType}
import feast.ingestion.stores.bigtable.serialization.Serializer
import org.apache.hadoop.security.UserGroupInformation

class BigTableSinkRelation(
    override val sqlContext: SQLContext,
    val serializer: Serializer,
    val config: SparkBigtableConfig,
    val hadoopConfig: Configuration
) extends BaseRelation
    with InsertableRelation
    with Serializable {

  import BigTableSinkRelation._

  override def schema: StructType = ???

  def createTable(): Unit = {
    val btConn = BigtableConfiguration.connect(hadoopConfig)
    try {
      val admin = btConn.getAdmin

      val table = if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
        val t          = new HTableDescriptor(TableName.valueOf(tableName))
        val metadataCF = new HColumnDescriptor(metadataColumnFamily)
        t.addFamily(metadataCF)
        t
      } else {
        admin.getTableDescriptor(TableName.valueOf(tableName))
      }

      if (!table.getColumnFamilyNames.contains(config.namespace.getBytes)) {
        val featuresCF = new HColumnDescriptor(config.namespace)
        if (config.maxAge > 0) {
          featuresCF.setTimeToLive(config.maxAge.toInt)
        }

        featuresCF.setMaxVersions(1)
        table.addFamily(featuresCF)
      }

      try {
        admin.createTable(table)
      } catch {
        case _: TableExistsException => admin.modifyTable(table)
      }
    } finally {
      btConn.close()
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val jobConfig = new JobConf(hadoopConfig)
    val jobCreds  = jobConfig.getCredentials()
    UserGroupInformation.setConfiguration(data.sqlContext.sparkContext.hadoopConfiguration)
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())

    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val featureFields = data.schema.fields
      .filterNot(f => isSystemColumn(f.name))

    val featureColumns = featureFields.map(f => col(f.name))

    val entityColumns   = config.entityColumns.map(c => col(c).cast(StringType))
    val schemaReference = serializer.schemaReference(StructType(featureFields))

    data
      .select(
        joinEntityKey(struct(entityColumns: _*)).alias("key"),
        serializer.serializeData(struct(featureColumns: _*)).alias("value"),
        col(config.timestampColumn).alias("ts")
      )
      .where(length(col("key")) > 0)
      .rdd
      .map(convertToPut(config.namespace.getBytes, emptyQualifier.getBytes, schemaReference))
      .saveAsHadoopDataset(jobConfig)

  }

  def saveWriteSchema(data: DataFrame): Unit = {
    val featureFields = data.schema.fields
      .filterNot(f => isSystemColumn(f.name))
    val featureSchema = StructType(featureFields)

    val key              = schemaKeyPrefix.getBytes ++ serializer.schemaReference(featureSchema)
    val serializedSchema = serializer.serializeSchema(featureSchema).getBytes

    val put       = new Put(key)
    val qualifier = "avro".getBytes
    put.addColumn(metadataColumnFamily.getBytes, qualifier, serializedSchema)

    val btConn = BigtableConfiguration.connect(hadoopConfig)
    try {
      val table = btConn.getTable(TableName.valueOf(tableName))
      table.checkAndPut(
        key,
        metadataColumnFamily.getBytes,
        qualifier,
        null,
        put
      )
    } finally {
      btConn.close()
    }
  }

  private def tableName: String = {
    val entities = config.entityColumns.mkString("__")
    s"${config.projectName}__${entities}"
  }

  private def joinEntityKey: UserDefinedFunction = udf { r: Row =>
    ((0 until r.size)).map(r.getString).mkString("#").getBytes
  }

  private val metadataColumnFamily = "metadata"
  private val schemaKeyPrefix      = "schema#"
  private val emptyQualifier       = ""

  private def isSystemColumn(name: String) =
    (config.entityColumns ++ Seq(config.timestampColumn)).contains(name)
}

object BigTableSinkRelation {
  def convertToPut(
      columnFamily: Array[Byte],
      column: Array[Byte],
      schemaReference: Array[Byte]
  ): Row => (Null, Put) =
    (r: Row) => {
      val put = new Put(r.getAs[Array[Byte]]("key"), r.getAs[java.sql.Timestamp]("ts").getTime)
      put.addColumn(
        columnFamily,
        column,
        schemaReference ++ r.getAs[Array[Byte]]("value")
      )
      (null, put)
    }
}
