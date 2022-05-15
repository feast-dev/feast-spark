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
package feast.ingestion.errorhanders

import scala.annotation.tailrec
import scala.concurrent.duration.Duration

object RetryStrategy {

  @tailrec
  def fixedBackOff[T <: Exception, U](retryInterval: Duration, maxRetries: Int)(
      fn: => Either[T, U]
  ): U = {
    fn match {
      case Right(x) => x
      case Left(_) if maxRetries > 0 => {
        Thread.sleep(retryInterval.toMillis)
        fixedBackOff(retryInterval, maxRetries - 1)(fn)
      }
      case Left(e) => throw (e)
    }
  }
}
