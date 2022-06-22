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

import akka.actor.{ ActorSystem, DynamicAccess }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ PartitionKeyResolver, TableNameResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V1AsyncClientFactory, V1SyncClientFactory }

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

final class V1DaxScalaDurableStateUpdateStoreFactory extends ScalaDurableStateUpdateStoreFactory {
  override def create[A](
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      pluginExecutor: ExecutionContext,
      partitionKeyResolver: PartitionKeyResolver,
      tableNameResolver: TableNameResolver,
      metricsReporter: Option[MetricsReporter],
      traceReporter: Option[TraceReporter],
      pluginConfig: StatePluginConfig
  ): ScalaDurableStateUpdateStore[A] = {
    val (maybeV1SyncClient, maybeV1AsyncClient) = pluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val f = dynamicAccess
          .createInstanceFor[V1SyncClientFactory](
            pluginConfig.v1SyncClientFactoryClassName,
            immutable.Seq.empty
          ) match {
          case Success(value) => value
          case Failure(ex) =>
            throw new PluginException("Failed to initialize V1SyncClientFactory", Some(ex))
        }
        val client = f.create(dynamicAccess, pluginConfig)
        (Some(client), None)
      case ClientType.Async =>
        val f = dynamicAccess
          .createInstanceFor[V1AsyncClientFactory](
            pluginConfig.v1AsyncClientFactoryClassName,
            immutable.Seq.empty
          ) match {
          case Success(value) => value
          case Failure(ex) =>
            throw new PluginException("Failed to initialize V1AsyncClientFactory", Some(ex))
        }
        val client = f.create(dynamicAccess, pluginConfig)
        (None, Some(client))
    }
    new DynamoDBDurableStateStoreV1(
      system,
      pluginExecutor,
      maybeV1AsyncClient,
      maybeV1SyncClient,
      partitionKeyResolver,
      tableNameResolver,
      metricsReporter,
      traceReporter,
      pluginConfig
    )
  }
}
