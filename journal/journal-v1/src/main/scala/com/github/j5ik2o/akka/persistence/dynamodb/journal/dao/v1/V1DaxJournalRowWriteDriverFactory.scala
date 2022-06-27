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
package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalPluginContext, JournalRowWriteDriverFactory }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{
  DynamicAccessUtils,
  V1DaxAsyncClientFactory,
  V1DaxSyncClientFactory
}

final class V1DaxJournalRowWriteDriverFactory(pluginContext: JournalPluginContext)
    extends JournalRowWriteDriverFactory {

  override def create: JournalRowWriteDriver = {
    val (maybeSyncClient, maybeAsyncClient) =
      pluginContext.pluginConfig.clientConfig.clientType match {
        case ClientType.Sync =>
          val f = DynamicAccessUtils.createInstanceFor_CTX_Throw[V1DaxSyncClientFactory, PluginContext](
            pluginContext.pluginConfig.v1DaxSyncClientFactoryClassName,
            pluginContext
          )
          val client = f.create
          (Some(client), None)
        case ClientType.Async =>
          val f = DynamicAccessUtils.createInstanceFor_CTX_Throw[V1DaxAsyncClientFactory, PluginContext](
            pluginContext.pluginConfig.v1DaxAsyncClientFactoryClassName,
            pluginContext
          )
          val client = f.create
          (None, Some(client))
      }
    new V1JournalRowWriteDriver(
      pluginContext,
      maybeAsyncClient,
      maybeSyncClient
    )
  }
}
