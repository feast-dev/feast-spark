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

import redis.clients.jedis.{ClusterPipeline, DefaultJedisClientConfig, HostAndPort}
import redis.clients.jedis.providers.ClusterConnectionProvider

import scala.collection.JavaConverters._

/**
  * Provide pipeline for Redis cluster.
  */
case class ClusterPipelineProvider(endpoint: RedisEndpoint) extends PipelineProvider {

  val nodes = Set(new HostAndPort(endpoint.host, endpoint.port)).asJava
  val DEFAULT_CLIENT_CONFIG = DefaultJedisClientConfig
    .builder()
    .password(endpoint.password)
    .build()
  val provider = new ClusterConnectionProvider(nodes, DEFAULT_CLIENT_CONFIG)

  /**
    * @return a cluster pipeline wrapped within the unified interface
    */
  override def pipeline(): UnifiedPipeline = new ClusterPipeline(provider)

}
