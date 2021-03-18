package feast.ingestion

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import feast.ingestion.helpers.DataHelper.{generateDistinctRows, rowGenerator, storeAsParquet}
import feast.ingestion.helpers.TestRow
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.SparkConf
import org.joda.time.DateTime

class BigTableIngestionSpec extends SparkSpec with ForAllTestContainer{
  override val container = GenericContainer(
    "google/cloud-sdk:latest",
    exposedPorts = Seq(8086),
    command = Seq("gcloud beta emulators bigtable start --host-port=0.0.0.0:8086")
  )

  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.bigtable.projectId", "null")
    .set("spark.bigtable.instanceId", "null")
    .set("spark.bigtable.emulatorHost", s"localhost:${container.mappedPort(8086)}")

  trait Scope {
    val config = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "test-fs",
        project = "default",
        entities = Seq(Field("customer", ValueType.Enum.STRING)),
        features = Seq(
          Field("feature1", ValueType.Enum.INT32),
          Field("feature2", ValueType.Enum.FLOAT)
        )
      ),
      startTime = DateTime.parse("2020-08-01"),
      endTime = DateTime.parse("2020-09-01"),
      store = BigTableConfig("", "")
    )

  }

  "Dataset" should "be ingested in BigTable" in new Scope {
    val gen      = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows     = generateDistinctRows(gen, 10000, (_:TestRow).customer)
    val tempPath = storeAsParquet(sparkSession, rows)
    val configWithOfflineSource = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp")
    )

    BatchPipeline.createPipeline(sparkSession, configWithOfflineSource)

  }
}
