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
package feast.ingestion.errorhandlers

import feast.ingestion.UnitSpec
import feast.ingestion.errorhanders.RetryStrategy

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Try}

class RetryStrategySpec extends UnitSpec {

  case class SomeException(message: String) extends Exception

  "function that always succeed" should "not be retried" in {
    var i = 0

    def alwaysSucceedFunc: Either[SomeException, Int] = {
      i += 1
      Right(0)
    }
    val result = RetryStrategy.fixedBackOff(1.second, 2)(alwaysSucceedFunc)
    i should be(1)
    result should be(0)
  }

  "function that always failed" should "be retried up to the maximum attempt and throw exception" in {
    var i = 0

    def alwaysFailFunc: Either[SomeException, Int] = {
      i += 1
      Left(SomeException("error"))
    }
    val result = Try { RetryStrategy.fixedBackOff(10.milli, 2)(alwaysFailFunc) }
    i should be(3)
    result should matchPattern { case Failure(_: SomeException) => }
  }

  "function that succeeds when retried" should "immediately return value when succeeded" in {
    var i = 0

    def succeedWhenRetriedFunc: Either[SomeException, Int] = {
      i += 1
      if (i < 2) {
        Left(SomeException("error"))
      } else {
        Right(0)
      }
    }
    val result = RetryStrategy.fixedBackOff(1.second, 2)(succeedWhenRetriedFunc)
    i should be(2)
    result should be(0)
  }
}
