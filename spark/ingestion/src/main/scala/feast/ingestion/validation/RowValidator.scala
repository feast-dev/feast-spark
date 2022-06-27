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
package feast.ingestion.validation

import feast.ingestion.{FeatureTable, ExpectationSpec}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

class RowValidator(
    featureTable: FeatureTable,
    timestampColumn: String,
    expectationSpec: Option[ExpectationSpec]
) extends Serializable {

  def allEntitiesPresent: Column =
    featureTable.entities.map(e => col(e.name).isNotNull).reduce(_.&&(_))

  def atLeastOneFeatureNotNull: Column =
    featureTable.features.map(f => col(f.name).isNotNull).reduce(_.||(_))

  def timestampPresent: Column =
    col(timestampColumn).isNotNull

  def validationChecks: Column = {

    expectationSpec match {
      case Some(value) if value.expectations.isEmpty => lit(true)
      case Some(value) =>
        value.expectations.map(_.validate).reduce(_.&&(_))
      case None => lit(true)
    }
  }

  def allChecks: Column =
    allEntitiesPresent && timestampPresent && validationChecks
}
