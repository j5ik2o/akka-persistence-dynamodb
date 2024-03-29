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
package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalPluginContext, JournalRowWriteDriverFactory }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V2DaxAsyncClientFactory, V2DaxSyncClientFactory }

final class V2DaxJournalRowWriteDriverFactory(pluginContext: JournalPluginContext)
    extends JournalRowWriteDriverFactory {

  override def create: JournalRowWriteDriver = {
    val (maybeSyncClient, maybeAsyncClient) =
      pluginContext.pluginConfig.clientConfig.clientType match {
        case ClientType.Sync =>
          val f = pluginContext
            .newDynamicAccessor[V2DaxSyncClientFactory]().createThrow(
              pluginContext.pluginConfig.v2DaxSyncClientFactoryClassName
            )
          val client = f.create
          (Some(client), None)
        case ClientType.Async =>
          val f = pluginContext
            .newDynamicAccessor[V2DaxAsyncClientFactory]().createThrow(
              pluginContext.pluginConfig.v2DaxAsyncClientFactoryClassName
            )
          val client = f.create
          (None, Some(client))
      }
    new V2JournalRowWriteDriver(
      pluginContext,
      maybeAsyncClient,
      maybeSyncClient
    )
  }

}
