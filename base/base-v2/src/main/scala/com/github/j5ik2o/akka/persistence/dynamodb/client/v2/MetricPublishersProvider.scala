/*
 * Copyright 2020 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessUtils
import software.amazon.awssdk.metrics.MetricPublisher

trait MetricPublishersProvider {
  def create: scala.collection.Seq[MetricPublisher]
}

object MetricPublishersProvider {

  def create(pluginContext: PluginContext): MetricPublishersProvider = {
    import pluginContext._
    val className = pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V2 =>
        pluginConfig.clientConfig.v2ClientConfig.metricPublishersProviderClassName
      case ClientVersion.V2Dax =>
        pluginConfig.clientConfig.v2DaxClientConfig.metricPublishersProviderClassName
    }
    DynamicAccessUtils.createInstanceFor_CTX_Throw[MetricPublishersProvider, PluginContext](
      className,
      pluginContext
    )
  }

  final class Default(pluginContext: PluginContext) extends MetricPublishersProvider {

    override def create: scala.collection.Seq[MetricPublisher] = {
      import pluginContext._
      val classNames = pluginConfig.clientConfig.clientVersion match {
        case ClientVersion.V2 =>
          pluginConfig.clientConfig.v2ClientConfig.metricPublisherClassNames
        case ClientVersion.V2Dax =>
          pluginConfig.clientConfig.v2DaxClientConfig.metricPublisherClassNames
      }
      classNames.map { className =>
        DynamicAccessUtils.createInstanceFor_CTX_Throw[MetricPublisher, PluginContext](
          className,
          pluginContext
        )
      }
    }
  }

}
