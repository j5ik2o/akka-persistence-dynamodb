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

import akka.actor.{ ActorSystem, DynamicAccess }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.journal.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{
  JournalRowWriteDriverFactory,
  PartitionKeyResolver,
  SortKeyResolver
}
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V2AsyncClientFactory, V2SyncClientFactory }

import scala.collection.immutable
import scala.util.{ Failure, Success }

final class V2JournalRowWriteDriverFactory extends JournalRowWriteDriverFactory {

  override def create(
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      journalPluginConfig: JournalPluginConfig,
      partitionKeyResolver: PartitionKeyResolver,
      sortKeyResolver: SortKeyResolver,
      metricsReporter: Option[MetricsReporter]
  ): JournalRowWriteDriver = {
    val (maybeSyncClient, maybeAsyncClient) =
      journalPluginConfig.clientConfig.clientType match {
        case ClientType.Sync =>
          val f = dynamicAccess
            .createInstanceFor[V2SyncClientFactory](
              journalPluginConfig.v2SyncClientFactoryClassName,
              immutable.Seq.empty
            ) match {
            case Success(value) => value
            case Failure(ex)    => throw new PluginException("Failed to initialize V2SyncClientFactory", Some(ex))
          }
          val client = f.create(dynamicAccess, journalPluginConfig)
          (Some(client), None)
        case ClientType.Async =>
          val f = dynamicAccess
            .createInstanceFor[V2AsyncClientFactory](
              journalPluginConfig.v2AsyncClientFactoryClassName,
              immutable.Seq.empty
            ) match {
            case Success(value) => value
            case Failure(ex)    => throw new PluginException("Failed to initialize V2AsyncClientFactory", Some(ex))
          }
          val client = f.create(dynamicAccess, journalPluginConfig)
          (None, Some(client))
      }
    new V2JournalRowWriteDriver(
      system,
      maybeAsyncClient,
      maybeSyncClient,
      journalPluginConfig,
      partitionKeyResolver,
      sortKeyResolver,
      metricsReporter
    )
  }

}
