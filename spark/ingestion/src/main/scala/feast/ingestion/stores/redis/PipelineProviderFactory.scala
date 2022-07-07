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

import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.util.Try

object PipelineProviderFactory {

  private lazy val providers: mutable.Map[RedisEndpoint, PipelineProvider] = mutable.Map.empty

  private def newJedisClient(endpoint: RedisEndpoint): Jedis = {
    val jedis = new Jedis(endpoint.host, endpoint.port)
    if (endpoint.password.nonEmpty) {
      jedis.auth(endpoint.password)
    }
    jedis
  }

  private def checkIfInClusterMode(endpoint: RedisEndpoint): Boolean = {
    val jedis     = newJedisClient(endpoint)
    val isCluster = Try(jedis.clusterInfo()).isSuccess
    jedis.close()
    isCluster
  }

  private def clusterPipelineProvider(endpoint: RedisEndpoint): PipelineProvider = {
    ClusterPipelineProvider(endpoint)
  }

  private def singleNodePipelineProvider(endpoint: RedisEndpoint): PipelineProvider = {
    SingleNodePipelineProvider(endpoint)
  }

  def newProvider(endpoint: RedisEndpoint): PipelineProvider = {
    if (checkIfInClusterMode(endpoint)) {
      clusterPipelineProvider(endpoint)
    }
    singleNodePipelineProvider(endpoint)
  }

  def provider(endpoint: RedisEndpoint): PipelineProvider = {
    providers.getOrElseUpdate(endpoint, newProvider(endpoint))
  }
}
