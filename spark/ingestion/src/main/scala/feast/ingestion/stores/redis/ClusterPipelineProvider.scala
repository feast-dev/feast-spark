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
package feast.ingestion.stores.redis

import feast.ingestion.errorhanders.RetryStrategy
import redis.clients.jedis.exceptions.JedisClusterOperationException
import redis.clients.jedis.{ClusterPipeline, DefaultJedisClientConfig, HostAndPort}
import redis.clients.jedis.providers.ClusterConnectionProvider

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

/**
  * Provide pipeline for Redis cluster.
  */
case class ClusterPipelineProvider(endpoint: RedisEndpoint) extends PipelineProvider {

  val nodes = Set(new HostAndPort(endpoint.host, endpoint.port)).asJava
  val DEFAULT_CLIENT_CONFIG = DefaultJedisClientConfig
    .builder()
    .password(endpoint.password)
    .build()
  val MAX_RECONNECTION_ATTEMPT = 2
  val RETRY_INTERVAL           = 2.seconds
  val provider                 = RetryStrategy.fixedBackOff(RETRY_INTERVAL, MAX_RECONNECTION_ATTEMPT)(getProvider)

  def getProvider: Either[JedisClusterOperationException, ClusterConnectionProvider] = {
    Try { new ClusterConnectionProvider(nodes, DEFAULT_CLIENT_CONFIG) } match {
      case Success(provider)                          => Right(provider)
      case Failure(e: JedisClusterOperationException) => Left(e)
      case Failure(e)                                 => throw e
    }
  }

  /**
    * @return a cluster pipeline
    */
  override def pipeline(): UnifiedPipeline = new ClusterPipeline(provider)

  /**
    * Close client connection
    */
  override def close(): Unit = {
    provider.close()
  }
}
