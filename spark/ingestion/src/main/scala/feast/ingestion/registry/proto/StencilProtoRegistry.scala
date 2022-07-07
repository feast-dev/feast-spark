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
package feast.ingestion.registry.proto
import com.google.protobuf.Descriptors
import io.odpf.stencil.StencilClientFactory
import io.odpf.stencil.client.StencilClient
import io.odpf.stencil.config.StencilConfig
import org.apache.http.{Header, HttpHeaders}
import org.apache.http.message.BasicHeader

import scala.collection.JavaConverters._

class StencilProtoRegistry(url: String, token: Option[String]) extends ProtoRegistry {
  import StencilProtoRegistry.stencilClient

  override def getProtoDescriptor(className: String): Descriptors.Descriptor = {
    stencilClient(url, token).get(className)
  }
}

object StencilProtoRegistry {
  @transient
  private var _stencilClient: StencilClient = _

  def stencilClient(url: String, token: Option[String]): StencilClient = {
    if (_stencilClient == null) {
      val stencilConfigBuilder = StencilConfig.builder
      for (t <- token) {
        val authHeader = new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + t)
        val headers    = List[Header](authHeader)
        stencilConfigBuilder.fetchHeaders(headers.asJava)
      }
      val stencilConfig = stencilConfigBuilder.build()
      _stencilClient = StencilClientFactory.getClient(url, stencilConfig)
    }
    _stencilClient
  }
}
