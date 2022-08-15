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
package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.actor.ActorSystem
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

final case class StatePluginContext(system: ActorSystem, pluginConfig: StatePluginConfig) extends PluginContext {

  override type This = StatePluginContext

  override def newDynamicAccessor[A: ClassTag](): StateDynamicAccessor[A] = {
    StateDynamicAccessor[A](this)
  }

  val metricsReporter: Option[MetricsReporter] = {
    val metricsReporterProvider = MetricsReporterProvider.create(this)
    metricsReporterProvider.create
  }

  val traceReporter: Option[TraceReporter] = {
    val traceReporterProvider = TraceReporterProvider.create(this)
    traceReporterProvider.create
  }

  val pluginExecutor: ExecutionContext =
    pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V1 =>
        DispatcherUtils.newV1Executor(this)
      case ClientVersion.V2 =>
        DispatcherUtils.newV2Executor(this)
    }

  val partitionKeyResolver: PartitionKeyResolver = {
    val provider = PartitionKeyResolverProvider.create(this)
    provider.create
  }

  val sortKeyResolver: SortKeyResolver = {
    val provider = SortKeyResolverProvider.create(this)
    provider.create
  }

  val tableNameResolver: TableNameResolver = {
    val provider = TableNameResolverProvider.create(this)
    provider.create
  }
}
