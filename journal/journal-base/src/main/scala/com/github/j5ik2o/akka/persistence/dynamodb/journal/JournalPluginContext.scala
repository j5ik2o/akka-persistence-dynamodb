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
package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.actor.ActorSystem
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.journal.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

final case class JournalPluginContext(system: ActorSystem, pluginConfig: JournalPluginConfig) extends PluginContext {
  override type This = JournalPluginContext

  override def newDynamicAccessor[A: ClassTag](): JournalDynamicAccessor[A] = {
    JournalDynamicAccessor[A](this)
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
      case ClientVersion.V1 => DispatcherUtils.newV1Executor(this)
      case ClientVersion.V2 => DispatcherUtils.newV2Executor(this)
    }

  val partitionKeyResolver: PartitionKeyResolver = {
    val provider = PartitionKeyResolverProvider.create(this)
    provider.create
  }

  val sortKeyResolver: SortKeyResolver = {
    val provider = SortKeyResolverProvider.create(this)
    provider.create
  }

  val journalRowWriteDriver: JournalRowWriteDriver = {
    val className = pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V1 =>
        pluginConfig.v1JournalRowWriteDriverFactoryClassName
      case ClientVersion.V1Dax =>
        pluginConfig.v1DaxJournalRowWriteDriverFactoryClassName
      case ClientVersion.V2 =>
        pluginConfig.v2JournalRowWriteDriverFactoryClassName
      case ClientVersion.V2Dax =>
        pluginConfig.v2DaxJournalRowWriteDriverFactoryClassName
    }
    val f = newDynamicAccessor[JournalRowWriteDriverFactory]().createThrow(className)
    f.create
  }

}
