package feast.ingestion.stores.bigtable


case class SparkBigtableConfig (
  namespace: String,
                                 projectName: String,
                                 entityColumns: Array[String],
                                 timestampColumn: String,
  maxAge: Long,
                               )
object SparkBigtableConfig {
  val NAMESPACE = "namespace"
  val ENTITY_COLUMNS = "entity_columns"
  val TS_COLUMN = "timestamp_column"
  val PROJECT_NAME = "project_name"
  val MAX_AGE = "max_age"

  def parse(parameters: Map[String, String]): SparkBigtableConfig =
    SparkBigtableConfig(
      namespace = parameters.getOrElse(NAMESPACE, ""),
      projectName = parameters.getOrElse(PROJECT_NAME, "default"),
      entityColumns = parameters.getOrElse(ENTITY_COLUMNS, "").split(","),
      timestampColumn = parameters.getOrElse(TS_COLUMN, "event_timestamp"),
      maxAge = parameters.get(MAX_AGE).map(_.toLong).getOrElse(0)
    )
}