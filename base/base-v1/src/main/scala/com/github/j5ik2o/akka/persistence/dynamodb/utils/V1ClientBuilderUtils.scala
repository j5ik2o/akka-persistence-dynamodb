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
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsyncClientBuilder, AmazonDynamoDBClientBuilder }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1._
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext

object V1ClientBuilderUtils {

  def setupSync(pluginContext: PluginContext): AmazonDynamoDBClientBuilder = {
    import pluginContext._
    val cc = V1ClientConfigurationUtils.setup(pluginContext)

    val csmConfigurationProviderProvider = CsmConfigurationProviderProvider.create(pluginContext)
    val monitoringListenerProvider       = MonitoringListenerProvider.create(pluginContext)
    val requestHandlersProvider          = RequestHandlersProvider.create(pluginContext)
    val requestMetricCollectorProvider   = RequestMetricCollectorProvider.create(pluginContext)
    val credentialsProviderProvider      = AWSCredentialsProviderProvider.create(pluginContext)

    val builder = AmazonDynamoDBClientBuilder.standard().withClientConfiguration(cc)

    csmConfigurationProviderProvider.create.foreach { c =>
      builder.setClientSideMonitoringConfigurationProvider(c)
    }
    monitoringListenerProvider.create.foreach { m =>
      builder.setMonitoringListener(m)
    }
    builder.setRequestHandlers(requestHandlersProvider.create: _*)
    requestMetricCollectorProvider.create.foreach { r =>
      builder.setMetricsCollector(r)
    }

    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.setCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
        )
      case _ =>
        credentialsProviderProvider.create.foreach { cp =>
          builder.setCredentials(cp)
        }
    }
    (pluginConfig.clientConfig.region, pluginConfig.clientConfig.endpoint) match {
      case (Some(r), Some(e)) =>
        builder.setEndpointConfiguration(new EndpointConfiguration(e, r))
      case (Some(r), _) =>
        builder.setRegion(r)
      case _ =>
    }
    builder
  }

  def setupAsync(
      pluginContext: PluginContext
  ): AmazonDynamoDBAsyncClientBuilder = {
    import pluginContext._
    val cc = V1ClientConfigurationUtils.setup(pluginContext)

    val csmConfigurationProviderProvider = CsmConfigurationProviderProvider.create(pluginContext)
    val monitoringListenerProvider       = MonitoringListenerProvider.create(pluginContext)
    val requestHandlersProvider          = RequestHandlersProvider.create(pluginContext)
    val requestMetricCollectorProvider   = RequestMetricCollectorProvider.create(pluginContext)

    val builder = AmazonDynamoDBAsyncClientBuilder.standard().withClientConfiguration(cc)

    csmConfigurationProviderProvider.create.foreach { c =>
      builder.setClientSideMonitoringConfigurationProvider(c)
    }
    monitoringListenerProvider.create.foreach { m =>
      builder.setMonitoringListener(m)
    }
    builder.setRequestHandlers(requestHandlersProvider.create: _*)
    requestMetricCollectorProvider.create.foreach { r =>
      builder.setMetricsCollector(r)
    }

    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.setCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
        )
      case _ =>
        val credentialsProviderProvider =
          AWSCredentialsProviderProvider.create(pluginContext)
        credentialsProviderProvider.create.foreach { cp =>
          builder.setCredentials(cp)
        }
    }

    (pluginConfig.clientConfig.region, pluginConfig.clientConfig.endpoint) match {
      case (Some(r), Some(e)) =>
        builder.setEndpointConfiguration(new EndpointConfiguration(e, r))
      case (Some(r), _) =>
        builder.setRegion(r)
      case _ =>
    }
    builder
  }

}
