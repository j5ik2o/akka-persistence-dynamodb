/*
 * Copyright 2022 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ StateDynamicAccessor, StatePluginContext }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V1AsyncClientFactory, V1SyncClientFactory }

final class V1DaxScalaDurableStateUpdateStoreFactory(pluginContext: StatePluginContext)
    extends ScalaDurableStateUpdateStoreFactory {
  override def create[A]: ScalaDurableStateUpdateStore[A] = {
    import pluginContext._
    val (maybeV1SyncClient, maybeV1AsyncClient) = pluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val f = StateDynamicAccessor[V1SyncClientFactory](pluginContext).createThrow(
          pluginConfig.v1SyncClientFactoryClassName
        )
        val client = f.create
        (Some(client), None)
      case ClientType.Async =>
        val f = StateDynamicAccessor[V1AsyncClientFactory](pluginContext).createThrow(
          pluginConfig.v1AsyncClientFactoryClassName
        )
        val client = f.create
        (None, Some(client))
    }
    new DynamoDBDurableStateStoreV1(
      pluginContext,
      maybeV1AsyncClient,
      maybeV1SyncClient
    )
  }
}
