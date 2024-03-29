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

import redis.clients.jedis.commands.PipelineBinaryCommands
import redis.clients.jedis.{JedisPool, Response}

/**
  * Provide pipeline for single node Redis.
  */
case class SingleNodePipelineProvider(endpoint: RedisEndpoint) extends PipelineProvider {

  val jedisPool = new JedisPool(endpoint.host, endpoint.port)

  /**
    * @return execute command within a pipeline and return the result
    */
  override def withPipeline[T](ops: PipelineBinaryCommands => T): T = {
    val jedis = jedisPool.getResource
    if (endpoint.password.nonEmpty) {
      jedis.auth(endpoint.password)
    }
    val response = ops(jedis.pipelined())
    jedis.close()
    response
  }

  /**
    * Close client connection
    */
  override def close(): Unit = jedisPool.close()

}
