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

import com.dimafeng.testcontainers.{CassandraContainer, ForAllTestContainer}
import feast.ingestion.helpers.DataHelper._
import feast.ingestion.metrics.StatsDStub
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, SaveMode}
import org.joda.time.{DateTime, Seconds}
import org.scalacheck._

case class CassandraTestRow(
    customer: String,
    feature1: Int,
    feature2: Float,
    eventTimestamp: java.sql.Timestamp
)

class CassandraIT extends SparkSpec with ForAllTestContainer {

  val statsDStub = new StatsDStub

  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.metrics.conf.*.sink.statsd.port", statsDStub.port.toString)
    .set("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    .set("spark.cassandra.connection.host", container.host)
    .set("spark.cassandra.connection.port", container.mappedPort(9042).toString)
    .set(
      s"spark.sql.catalog.feast",
      "com.datastax.spark.connector.datasource.CassandraCatalog"
    )

  override val container: CassandraContainer = CassandraContainer()

  trait Scope {

    statsDStub.receivedMetrics // clean the buffer

    val keyspace = "feast"

    val session = container.cluster.newSession()
    session.execute(s"""
         |DROP KEYSPACE IF EXISTS ${keyspace}
         |""".stripMargin)
    session.execute(s"""
         |CREATE KEYSPACE ${keyspace} WITH REPLICATION = {
         |'class' : 'SimpleStrategy',
         |'replication_factor' : 1
         |};
         |""".stripMargin)

    implicit def testRowEncoder: Encoder[CassandraTestRow] = ExpressionEncoder()

    def rowGenerator(start: DateTime, end: DateTime) =
      for {
        customer <- Gen.uuid.map(_.toString)
        feature1 <- Gen.choose(0, 100)
        feature2 <- Gen.choose[Float](0, 1)
        eventTimestamp <- Gen
          .choose(0, Seconds.secondsBetween(start, end).getSeconds - 1)
          .map(start.withMillisOfSecond(0).plusSeconds)
      } yield CassandraTestRow(
        customer,
        feature1,
        feature2,
        new java.sql.Timestamp(eventTimestamp.getMillis)
      )

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
      metrics = Some(StatsDConfig(host = "localhost", port = statsDStub.port)),
      store = CassandraConfig(
        CassandraConnection(container.host, container.mappedPort(9042)),
        keyspace,
        CassandraWriteProperties(1024, 5)
      )
    )
  }

  "Parquet source file" should "be ingested in cassandra" in new Scope {
    val gen      = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows     = generateRows(gen, 1000)
    val tempPath = storeAsParquet(sparkSession, rows)
    val configWithOfflineSource = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp", datePartitionColumn = Some("date"))
    )

    BatchPipeline.createPipeline(sparkSession, configWithOfflineSource)

    session
      .execute(s"SELECT COUNT(1) AS cnt FROM ${keyspace}.default_customer")
      .one()
      .getLong("cnt") should equal(1000)
  }

}
