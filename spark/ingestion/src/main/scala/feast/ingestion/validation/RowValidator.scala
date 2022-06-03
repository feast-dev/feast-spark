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

import feast.ingestion.{FeatureTable, ValidationSpec, Expectation}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}
import org.json4s.jackson.JsonMethods.{parse => parseJSON}
import org.json4s._

class RowValidator(featureTable: FeatureTable, timestampColumn: String) extends Serializable {
  implicit val formats: Formats = DefaultFormats

  def allEntitiesPresent: Column =
    featureTable.entities.map(e => col(e.name).isNotNull).reduce(_.&&(_))

  def atLeastOneFeatureNotNull: Column =
    featureTable.features.map(f => col(f.name).isNotNull).reduce(_.||(_))

  def timestampPresent: Column =
    col(timestampColumn).isNotNull

  def expectColumnValuesToBeBetween(expectation: Expectation): Column = {
    val minValue: Option[String] = expectation.kwargs.get("minValue")
    val maxValue: Option[String] = expectation.kwargs.get("maxValue")

    (minValue, maxValue) match {
      case (Some(min), Some(max)) => col(expectation.kwargs("column")).between(min, max)
      case (Some(min), None) => col(expectation.kwargs("column")).>=(min)
      case (None, Some(max)) => col(expectation.kwargs("column")).<=(max)
      case _ => lit(true)
    }
  }

  def validate(expectation: Expectation): Column = {
    expectation.expectationType match {
      case "expect_column_values_to_not_be_null" => col(expectation.kwargs("column")).isNotNull
      case "expect_column_values_to_be_between" => expectColumnValuesToBeBetween(expectation)
      case _ => lit(true)
    }
  }

  def validationChecks: Column = {
    val validationSpec: Option[ValidationSpec] =
      featureTable.labels.get("_validation").map(parseJSON(_).camelizeKeys.extract[ValidationSpec])

    validationSpec match {
      case Some(value) if value.expectations.isEmpty => lit(true)
      case Some(value) => value.expectations.map(expectation => validate(expectation)).reduce(_.&&(_))
      case None => lit(true)
    }
  }

  def allChecks: Column =
    allEntitiesPresent && timestampPresent && validationChecks
}
