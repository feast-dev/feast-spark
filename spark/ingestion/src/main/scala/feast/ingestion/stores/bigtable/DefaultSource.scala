package feast.ingestion.stores.bigtable

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import com.google.cloud.bigtable.hbase.BigtableConfiguration

class DefaultSource extends CreatableRelationProvider{
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val bigtableConf = BigtableConfiguration.configure(
      sqlContext.getConf("spark.bigtable.projectId"),
      sqlContext.getConf("spark.bigtable.instanceId"))

    if (sqlContext.getConf("spark.bigtable.emulatorHost", "").nonEmpty) {
      bigtableConf.set(
        "google.bigtable.emulator.endpoint.host",
        sqlContext.getConf("spark.bigtable.emulatorHost"))
    }

    val rel = new BigTableSinkRelation(sqlContext, SparkBigtableConfig.parse(parameters), bigtableConf)
    rel.createTable()
    rel.saveWriteSchema(data)
    rel.insert(data, overwrite = false)
    rel
  }
}
