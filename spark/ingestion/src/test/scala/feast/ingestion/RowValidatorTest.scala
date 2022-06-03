/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2022 The Feast Authors
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

import com.example.protos.VehicleType
import feast.ingestion.validation.RowValidator
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

class RowValidatorTest extends SparkSpec {

  val featureTable: FeatureTable = FeatureTable(
    name = "driver-fs",
    project = "default",
    entities = Seq(
      Field("s2_id", ValueType.Enum.INT64),
      Field("vehicle_type", ValueType.Enum.STRING)
    ),
    features = Seq(
      Field("unique_drivers", ValueType.Enum.INT64)
    ),
    labels = Map(
      "_validation" -> "{\"expectations\": [{\"expectation_type\": \"expect_column_values_to_not_be_null\", \"kwargs\": { \"column\": \"unique_drivers\"}}, {\"expectation_type\": \"expect_column_values_to_be_between\", \"kwargs\": { \"column\": \"unique_drivers\", \"min_value\": 2, \"max_value\": 20}}]}"
    )
  )

  val rowValidator = new RowValidator(featureTable, "timestamp")

  val schema = List(
    StructField("s2_id", IntegerType, nullable = true),
    StructField("vehicle_type", StringType, nullable = true),
    StructField("unique_drivers", IntegerType, nullable = true),
    StructField("timestamp", StringType, nullable = true)
  )

  "timestampPresent" should "filter out records with no timestamps" in {
    val data = Seq(
      Row(1, VehicleType.Enum.BIKE.name(), 4, DateTime.now().toString()),
      Row(2, VehicleType.Enum.BIKE.name(), 8, DateTime.now().toString()),
      Row(3, VehicleType.Enum.CAR.name(), 5, DateTime.now().toString()),
      // missing timestamp
      Row(4, VehicleType.Enum.CAR.name(), 8, null)
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(true),
      Row(true),
      Row(true),
      Row(false)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.timestampPresent)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

  "allEntitiesPresent" should "filter out records with missing entity" in {
    val data = Seq(
      Row(1, VehicleType.Enum.BIKE.name(), 4, DateTime.now().toString()),
      Row(2, VehicleType.Enum.BIKE.name(), 8, DateTime.now().toString()),
      Row(3, VehicleType.Enum.CAR.name(), 5, DateTime.now().toString()),
      // missing entity
      Row(4, null, 2, DateTime.now().toString())
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(true),
      Row(true),
      Row(true),
      Row(false)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.allEntitiesPresent)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

  "validationChecks" should "filter out invalid rows" in {
    val data = Seq(
      Row(1, VehicleType.Enum.BIKE.name(), 4, DateTime.now().toString()),
      // feature is null
      Row(2, VehicleType.Enum.BIKE.name(), null, DateTime.now().toString()),
      // feature value outside of expected range in validation config
      Row(3, VehicleType.Enum.CAR.name(), 22, DateTime.now().toString()),
      Row(4, VehicleType.Enum.CAR.name(), 7, DateTime.now().toString()),
      // feature value outside of expected range in validation config
      Row(5, VehicleType.Enum.CAR.name(), -1, DateTime.now().toString())
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(true),
      Row(false),
      Row(false),
      Row(true),
      Row(false)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.validationChecks)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

  it should "should skip filtering if validation config doesnt exist" in {
    val featureTable: FeatureTable = FeatureTable(
      name = "driver-fs",
      project = "default",
      entities = Seq(
        Field("s2_id", ValueType.Enum.INT64),
        Field("vehicle_type", ValueType.Enum.STRING)
      ),
      features = Seq(
        Field("unique_drivers", ValueType.Enum.INT64)
      ),
      // validation config not present
      labels = Map()
    )

    val rowValidator = new RowValidator(featureTable, "timestamp")

    val data = Seq(
      Row(1, VehicleType.Enum.BIKE.name(), 4, DateTime.now().toString()),
      Row(2, VehicleType.Enum.BIKE.name(), null, DateTime.now().toString()),
      Row(3, VehicleType.Enum.CAR.name(), 22, DateTime.now().toString()),
      Row(4, VehicleType.Enum.CAR.name(), 7, DateTime.now().toString())
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(true),
      Row(true),
      Row(true),
      Row(true)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.validationChecks)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

  it should "should skip filtering if validation config doesnt contain expectations" in {
    val featureTable: FeatureTable = FeatureTable(
      name = "driver-fs",
      project = "default",
      entities = Seq(
        Field("s2_id", ValueType.Enum.INT64),
        Field("vehicle_type", ValueType.Enum.STRING)
      ),
      features = Seq(
        Field("unique_drivers", ValueType.Enum.INT64)
      ),
      // expectations not present in _validation json
      labels = Map(
        "_validation"             -> "{\"name\": \"testUDF\", \"pickled_code_path\": \"gs://feast-dataproc-staging-batch/test-staging/b0f4ffa4-a3bf-440a-9d50-2dd1ae5d0431/36a316ce-3797-4202-830b-adab40902ceb/dataproc/udfs/validation_ge/testUDF.pickle\", \"include_archive_path\": \"https://storage.googleapis.com/feast-jobs/spark/validation/pylibs-ge-%(platform)s.tar.gz\"}",
        "_streaming_trigger_secs" -> "1"
      )
    )

    val rowValidator = new RowValidator(featureTable, "timestamp")

    val data = Seq(
      Row(1, VehicleType.Enum.BIKE.name(), 4, DateTime.now().toString()),
      Row(2, VehicleType.Enum.BIKE.name(), null, DateTime.now().toString()),
      Row(3, VehicleType.Enum.CAR.name(), 22, DateTime.now().toString()),
      Row(4, VehicleType.Enum.CAR.name(), 7, DateTime.now().toString())
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(true),
      Row(true),
      Row(true),
      Row(true)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.validationChecks)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

  "expectColumnValuesToBeBetween" should "perform a greater than check if only minValue is provided" in {
    val featureTable: FeatureTable = FeatureTable(
      name = "driver-fs",
      project = "default",
      entities = Seq(
        Field("s2_id", ValueType.Enum.INT64),
        Field("vehicle_type", ValueType.Enum.STRING)
      ),
      features = Seq(
        Field("unique_drivers", ValueType.Enum.INT64),
        Field("average_rating", ValueType.Enum.FLOAT)
      ),
      // only min_value present in expect_column_values_to_be_between check
      labels = Map(
        "_validation" -> "{\"expectations\": [{\"expectation_type\": \"expect_column_values_to_be_between\", \"kwargs\": { \"column\": \"average_rating\", \"min_value\": 4}}]}"
      )
    )

    val rowValidator = new RowValidator(featureTable, "timestamp")

    val data = Seq(
      Row(1, VehicleType.Enum.BIKE.name(), 25, 4.1, DateTime.now().toString()),
      // feature value - average_rating is less than min value(4)
      Row(2, VehicleType.Enum.BIKE.name(), 10, 3.8, DateTime.now().toString()),
      Row(3, VehicleType.Enum.CAR.name(), 25, 4.2, DateTime.now().toString()),
      Row(4, VehicleType.Enum.CAR.name(), 17, 4.2, DateTime.now().toString())
    )

    val schema = List(
      StructField("s2_id", IntegerType, nullable = true),
      StructField("vehicle_type", StringType, nullable = true),
      StructField("unique_drivers", IntegerType, nullable = true),
      StructField("average_rating", DoubleType, nullable = true),
      StructField("timestamp", StringType, nullable = true)
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(true),
      Row(false),
      Row(true),
      Row(true)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.validationChecks)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

  it should "perform a less than check if only maxValue is provided" in {
    val featureTable: FeatureTable = FeatureTable(
      name = "driver-fs",
      project = "default",
      entities = Seq(
        Field("s2_id", ValueType.Enum.INT64),
        Field("vehicle_type", ValueType.Enum.STRING)
      ),
      features = Seq(
        Field("unique_drivers", ValueType.Enum.INT64),
        Field("average_rating", ValueType.Enum.FLOAT)
      ),
      // only max_value present in expect_column_values_to_be_between check
      labels = Map(
        "_validation" -> "{\"expectations\": [{\"expectation_type\": \"expect_column_values_to_be_between\", \"kwargs\": { \"column\": \"unique_drivers\", \"max_value\": 25}}]}"
      )
    )

    val rowValidator = new RowValidator(featureTable, "timestamp")

    val data = Seq(
      // feature value - unique_drivers is greater than max value(25)
      Row(1, VehicleType.Enum.BIKE.name(), 75, 4.1, DateTime.now().toString()),
      Row(2, VehicleType.Enum.BIKE.name(), 10, 3.8, DateTime.now().toString()),
      // feature value - unique_drivers is greater than max value(25)
      Row(3, VehicleType.Enum.CAR.name(), 26, 4.2, DateTime.now().toString()),
      Row(4, VehicleType.Enum.CAR.name(), 17, 4.2, DateTime.now().toString())
    )

    val schema = List(
      StructField("s2_id", IntegerType, nullable = true),
      StructField("vehicle_type", StringType, nullable = true),
      StructField("unique_drivers", IntegerType, nullable = true),
      StructField("average_rating", DoubleType, nullable = true),
      StructField("timestamp", StringType, nullable = true)
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(false),
      Row(true),
      Row(false),
      Row(true)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.validationChecks)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

  it should "should skip filtering if both min and max values are missing" in {
    val featureTable: FeatureTable = FeatureTable(
      name = "driver-fs",
      project = "default",
      entities = Seq(
        Field("s2_id", ValueType.Enum.INT64),
        Field("vehicle_type", ValueType.Enum.STRING)
      ),
      features = Seq(
        Field("unique_drivers", ValueType.Enum.INT64)
      ),
      // both min_value and max_value are absent in expect_column_values_to_be_between check
      labels = Map(
        "_validation" -> "{\"expectations\": [{\"expectation_type\": \"expect_column_values_to_be_between\", \"kwargs\": { \"column\": \"unique_drivers\"}}]}"
      )
    )

    val rowValidator = new RowValidator(featureTable, "timestamp")

    val data = Seq(
      Row(1, VehicleType.Enum.BIKE.name(), -19, DateTime.now().toString()),
      Row(2, VehicleType.Enum.BIKE.name(), null, DateTime.now().toString()),
      Row(3, VehicleType.Enum.CAR.name(), 999, DateTime.now().toString()),
      Row(4, VehicleType.Enum.CAR.name(), 0, DateTime.now().toString())
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(true),
      Row(true),
      Row(true),
      Row(true)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.validationChecks)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

  "allChecks" should "filter out records where any check fails" in {
    val data = Seq(
      Row(1, VehicleType.Enum.BIKE.name(), 4, DateTime.now().toString()),
      Row(2, VehicleType.Enum.BIKE.name(), 8, DateTime.now().toString()),
      // timestamp is null
      Row(3, VehicleType.Enum.CAR.name(), 5, null),
      // entity null
      Row(4, null, 2, DateTime.now().toString()),
      // entity null
      Row(null, VehicleType.Enum.BIKE.name(), 8, DateTime.now().toString()),
      // feature out of range
      Row(5, VehicleType.Enum.BIKE.name(), 1, DateTime.now().toString()),
      // feature out of range
      Row(6, VehicleType.Enum.CAR.name(), 98, DateTime.now().toString()),
      Row(6, VehicleType.Enum.BIKE.name(), 20, DateTime.now().toString()),
      // feature out of range, timestamp null
      Row(7, VehicleType.Enum.BIKE.name(), 87, null)
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    val expectedIsValidColumn = Seq(
      Row(true),
      Row(true),
      Row(false),
      Row(false),
      Row(false),
      Row(false),
      Row(false),
      Row(true),
      Row(false)
    )

    val rowsAfterValidation = df.withColumn("_isValid", rowValidator.allChecks)
    rowsAfterValidation.select("_isValid").collect().toList shouldBe expectedIsValidColumn
  }

}
