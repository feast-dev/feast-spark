package feast.ingestion.stores.bigtable

import com.google.cloud.bigtable.hbase.BigtableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType

import com.google.common.hash.Hashing

class BigTableSinkRelation(override val sqlContext: SQLContext,
                           val config: SparkBigtableConfig,
                           val hadoopConfig: Configuration)
  extends BaseRelation with InsertableRelation with Serializable {

  override def schema: StructType = ???

  def createTable(): Unit = {
    val btConn = BigtableConfiguration.connect(hadoopConfig)
    val admin = btConn.getAdmin
    if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))

      val featuresCF = new HColumnDescriptor(featureColumnFamily)
      featuresCF.setTimeToLive(config.maxAge.toInt)
      featuresCF.setVersions(0, 1)
      tableDesc.addFamily(featuresCF)

      val metadataCF = new HColumnDescriptor(metadataColumnFamily)
      tableDesc.addFamily(metadataCF)

      admin.createTable(tableDesc)
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val jobConfig: JobConf = new JobConf(hadoopConfig, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val featureColumns = data.schema.fields.map(_.name).filterNot(
      isSystemColumn
    ).map(col)

    val entityColumns = config.entityColumns.map(col)
    val schemaReference = writeSchemaReference(data.schema)

    data
      .select(
        convertToPut(schemaReference)(
          joinEntityKey(struct(entityColumns:_*)),
          to_avro(struct(featureColumns:_*)),
          col(config.timestampColumn)
        )
      ).rdd
      .map{ r: Row => (r.get(0), r.get(1))}
      .saveAsHadoopDataset(jobConfig)

  }

  private def writeSchema(schema: StructType): String = {
    val avroSchema = SchemaConverters.toAvroType(
      StructType(
        // excluding entities & timestamp, so the schema would match to what we actually write as value
        schema.fields.filterNot(f => isSystemColumn(f.name))
      )
    )
    avroSchema.toString
  }

  private def writeSchemaReference(schema: StructType): Array[Byte] = {
    Hashing.murmur3_32().hashBytes(writeSchema(schema).getBytes).asBytes()
  }

  def saveWriteSchema(data: DataFrame): Unit = {
    val btConn = BigtableConfiguration.connect(hadoopConfig)
    val table = btConn.getTable(TableName.valueOf(tableName))
    val key = s"schema#".getBytes ++ writeSchemaReference(data.schema)

    val put = new Put(key)
    put.addColumn(metadataColumnFamily.getBytes, "json".getBytes, writeSchema(data.schema).getBytes)

    table.checkAndPut(
      key,
      metadataColumnFamily.getBytes,
      "json".getBytes,
      null,
      put
    )

  }


  private def tableName: String = {
    val entities = config.entityColumns.mkString("_")
    s"${config.projectName}_${entities}"
  }

  private def joinEntityKey: UserDefinedFunction = udf {
    r: Row => ((0 until r.size)).map(r.getString).mkString("#")
  }

  private def convertToPut(schemaReference: Array[Byte]): UserDefinedFunction = udf {
    (key: Array[Byte], value: Array[Byte], ts: java.sql.Timestamp) =>
      val put = new Put(key, ts.getTime)
      put.addColumn(
        featureColumnFamily.getBytes,
        config.namespace.getBytes,
        schemaReference ++ value
      )
      (new ImmutableBytesWritable, put)
  }

  private val featureColumnFamily = "features"
  private val metadataColumnFamily = "metadata"

  private def isSystemColumn(name: String) =
    (config.entityColumns ++ Seq(config.timestampColumn)).contains(name)
}
