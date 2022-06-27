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
package feast.ingestion.validation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JObject, JValue}

trait Expectation {

  def validate: Column
}

case class ExpectColumnValuesToNotBeNull(columnName: String) extends Expectation {
  override def validate: Column = col(columnName).isNotNull
}

case class ExpectColumnValuesToBeBetween(
    columnName: String,
    minValue: Option[Int],
    maxValue: Option[Int]
) extends Expectation {
  override def validate: Column = {
    (minValue, maxValue) match {
      case (Some(min), Some(max)) => col(columnName).between(min, max)
      case (Some(min), None)      => col(columnName).>=(min)
      case (None, Some(max))      => col(columnName).<=(max)
      case _                      => lit(true)
    }
  }
}

object Expectation {
  implicit val format: Formats = DefaultFormats

  def extractColumn(kwargs: JValue): String = {
    (kwargs \ "column").extract[String]
  }

  def apply(expectationType: String, kwargs: JValue): Expectation = {
    expectationType match {
      case "expect_column_values_to_not_be_null" =>
        ExpectColumnValuesToNotBeNull(extractColumn(kwargs))
      case "expect_column_values_to_be_between" =>
        val column   = extractColumn(kwargs)
        val minValue = (kwargs \ "minValue").toSome.map(_.extract[Int])
        val maxValue = (kwargs \ "maxValue").toSome.map(_.extract[Int])
        ExpectColumnValuesToBeBetween(column, minValue, maxValue)
    }
  }
}

object ExpectationCodec
    extends CustomSerializer[Expectation](implicit format =>
      (
        { case x: JObject =>
          val eType: String  = (x \ "expectationType").extract[String]
          val kwargs: JValue = (x \ "kwargs")
          Expectation(eType, kwargs)
        },
        { case x: Expectation =>
          Extraction.decompose(x)
        }
      )
    )
