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
package feast.ingestion

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.google.cloud.bigtable.hbase.BigtableConfiguration
import feast.ingestion.helpers.DataHelper.{generateDistinctRows, rowGenerator, storeAsParquet}
import feast.ingestion.helpers.TestRow
import feast.proto.types.ValueProto.ValueType
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Scan}
import org.apache.spark.SparkConf
import org.joda.time.DateTime

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

class BigTableIngestionSpec extends SparkSpec with ForAllTestContainer {
  override val container = GenericContainer(
    "google/cloud-sdk:latest",
    exposedPorts = Seq(8086),
    command = Seq("gcloud", "beta", "emulators", "bigtable", "start", "--host-port", "0.0.0.0:8086")
  )

  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.bigtable.projectId", "null")
    .set("spark.bigtable.instanceId", "null")
    .set("spark.bigtable.emulatorHost", s"localhost:${container.mappedPort(8086)}")

  def withBTConnection(testCode: Connection => Any): Unit = {
    val btConf = BigtableConfiguration.configure("null", "null")
    btConf.set("google.bigtable.emulator.endpoint.host", s"localhost:${container.mappedPort(8086)}")
    val btConn = BigtableConfiguration.connect(btConf)

    try {
      testCode(btConn)
    } finally {
      btConn.close()
    }
  }

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

    val gen = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))

    def decodeAvroValue(value: Array[Byte], jsonFormatSchema: String): GenericRecord = {
      val input = value.drop(4) // drop schema reference

      val schema      = new Schema.Parser().parse(jsonFormatSchema)
      val reader      = new GenericDatumReader[Any](schema)
      var result: Any = null

      val decoder = DecoderFactory.get().binaryDecoder(input, 0, input.length, null)
      result = reader.read(result, decoder)
      result.asInstanceOf[GenericRecord]
    }

  }

  "Dataset" should "be ingested in BigTable" in withBTConnection { btConn =>
    new Scope {
      val rows     = generateDistinctRows(gen, 10000, (_: TestRow).customer)
      val tempPath = storeAsParquet(sparkSession, rows)
      val configWithOfflineSource = config.copy(
        source = FileSource(tempPath, Map.empty, "eventTimestamp")
      )

      BatchPipeline.createPipeline(sparkSession, configWithOfflineSource)

      val table       = btConn.getTable(TableName.valueOf("default_customer"))
      val allRowsInBT = table.getScanner(new Scan).asScala.toList

      val schema = allRowsInBT
        .filter(_.getRow.startsWith("schema".getBytes))
        .map(r => new String(r.value))
        .head

      val storedRows = allRowsInBT
        .filterNot(_.getRow.startsWith("schema".getBytes))
        .map(r => {
          val cell   = r.getColumnLatestCell("test-fs".getBytes, "".getBytes)
          val record = decodeAvroValue(cell.getValueArray, schema)
          TestRow(
            new String(r.getRow),
            record.get("feature1").asInstanceOf[Integer],
            record.get("feature2").asInstanceOf[Float],
            new java.sql.Timestamp(cell.getTimestamp)
          )
        })

      storedRows should contain allElementsOf rows.filterNot(_.customer.isEmpty)
    }
  }

  "Column family" should "be created with appropriate ttl" in withBTConnection { btConn =>
    new Scope {
      val rows     = generateDistinctRows(gen, 1, (_: TestRow).customer)
      val tempPath = storeAsParquet(sparkSession, rows)
      val configWithOfflineSource = config.copy(
        source = FileSource(tempPath, Map.empty, "eventTimestamp"),
        featureTable = config.featureTable.copy(
          name = "feature-table-name",
          maxAge = Some(600L)
        )
      )

      BatchPipeline.createPipeline(sparkSession, configWithOfflineSource)

      val table = btConn.getAdmin.getDescriptor(TableName.valueOf("default_customer"))
      val cf    = table.getColumnFamily(configWithOfflineSource.featureTable.name.getBytes)
      cf.getTimeToLive should be(600)
      cf.getMaxVersions should be(1)
      cf.getMinVersions should be(0)
    }
  }
}
