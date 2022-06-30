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
package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao

import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.{ SnapshotDynamicAccessor, SnapshotPluginContext }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V2AsyncClientFactory, V2SyncClientFactory }

final class V2SnapshotDaoFactory(pluginContext: SnapshotPluginContext) extends SnapshotDaoFactory {
  override def create(
      serialization: Serialization
  ): SnapshotDao = {
    import pluginContext._
    val (async, sync) = pluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val f = SnapshotDynamicAccessor[V2SyncClientFactory](pluginContext).createThrow(
          pluginConfig.v2SyncClientFactoryClassName
        )
        val v1JavaSyncClient = f.create
        (None, Some(v1JavaSyncClient))
      case ClientType.Async =>
        val f = SnapshotDynamicAccessor[V2AsyncClientFactory](pluginContext).createThrow(
          pluginConfig.v2AsyncClientFactoryClassName
        )
        val v1JavaAsyncClient = f.create
        (Some(v1JavaAsyncClient), None)
    }
    if (pluginConfig.legacyTableFormat)
      new V2LegacySnapshotDaoImpl(pluginContext, async, sync, serialization)
    else
      new V2NewSnapshotDaoImpl(
        pluginContext,
        async,
        sync,
        serialization
      )
  }
}
