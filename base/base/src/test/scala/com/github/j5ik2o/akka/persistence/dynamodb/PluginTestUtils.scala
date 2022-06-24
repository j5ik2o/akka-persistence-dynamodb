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
package com.github.j5ik2o.akka.persistence.dynamodb

import akka.actor.ActorSystem
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporterProvider
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporterProvider

object PluginTestUtils {
  def newPluginContext(
      _system: ActorSystem,
      metricsReporterClass: Option[Class[_]],
      traceReporterClass: Option[Class[_]]
  ): PluginContext =
    new PluginContext {
      override val pluginConfig: PluginConfig = new PluginConfig {
        override val configRootPath: String                   = null
        override val v1AsyncClientFactoryClassName: String    = null
        override val v1SyncClientFactoryClassName: String     = null
        override val v1DaxAsyncClientFactoryClassName: String = null
        override val v1DaxSyncClientFactoryClassName: String  = null
        override val v2AsyncClientFactoryClassName: String    = null
        override val v2SyncClientFactoryClassName: String     = null
        override val tableName: String                        = null
        override val metricsReporterProviderClassName: String = classOf[MetricsReporterProvider.Default].getName
        override val metricsReporterClassName: Option[String] = metricsReporterClass.map(_.getName)
        override val traceReporterProviderClassName: String   = classOf[TraceReporterProvider.Default].getName
        override val traceReporterClassName: Option[String]   = traceReporterClass.map(_.getName)
        override val clientConfig: DynamoDBClientConfig       = null
      }

      override def system: ActorSystem = _system
    }
}
