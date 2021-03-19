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

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import com.google.cloud.bigtable.hbase.BigtableConfiguration
import feast.ingestion.stores.bigtable.serialization.AvroSerializer

class DefaultSource extends CreatableRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    val bigtableConf = BigtableConfiguration.configure(
      sqlContext.getConf("spark.bigtable.projectId"),
      sqlContext.getConf("spark.bigtable.instanceId")
    )

    if (sqlContext.getConf("spark.bigtable.emulatorHost", "").nonEmpty) {
      bigtableConf.set(
        "google.bigtable.emulator.endpoint.host",
        sqlContext.getConf("spark.bigtable.emulatorHost")
      )
    }

    val rel =
      new BigTableSinkRelation(sqlContext, new AvroSerializer, SparkBigtableConfig.parse(parameters), bigtableConf)
    rel.createTable()
    rel.saveWriteSchema(data)
    rel.insert(data, overwrite = false)
    rel
  }
}
