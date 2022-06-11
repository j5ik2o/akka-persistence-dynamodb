package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsyncClientBuilder, AmazonDynamoDBClientBuilder }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1._
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

object V1DynamoDBClientBuilderUtils {

  def setupSync(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDBClientBuilder = {
    val cc = V1ClientConfigurationUtils.setup(dynamicAccess, pluginConfig)

    val csmConfigurationProviderProvider = CsmConfigurationProviderProvider.create(dynamicAccess, pluginConfig)
    val monitoringListenerProvider       = MonitoringListenerProvider.create(dynamicAccess, pluginConfig)
    val requestHandlersProvider          = RequestHandlersProvider.create(dynamicAccess, pluginConfig)
    val requestMetricCollectorProvider   = RequestMetricCollectorProvider.create(dynamicAccess, pluginConfig)
    val credentialsProviderProvider      = AWSCredentialsProviderProvider.create(dynamicAccess, pluginConfig)

    val builder = AmazonDynamoDBClientBuilder.standard().withClientConfiguration(cc)

    csmConfigurationProviderProvider.create.foreach { c => builder.setClientSideMonitoringConfigurationProvider(c) }
    monitoringListenerProvider.create.foreach { m => builder.setMonitoringListener(m) }
    builder.setRequestHandlers(requestHandlersProvider.create: _*)
    requestMetricCollectorProvider.create.foreach { r => builder.setMetricsCollector(r) }

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

  def setupAsync(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDBAsyncClientBuilder = {
    val cc = V1ClientConfigurationUtils.setup(dynamicAccess, pluginConfig)

    val csmConfigurationProviderProvider = CsmConfigurationProviderProvider.create(dynamicAccess, pluginConfig)
    val monitoringListenerProvider       = MonitoringListenerProvider.create(dynamicAccess, pluginConfig)
    val requestHandlersProvider          = RequestHandlersProvider.create(dynamicAccess, pluginConfig)
    val requestMetricCollectorProvider   = RequestMetricCollectorProvider.create(dynamicAccess, pluginConfig)
    val credentialsProviderProvider      = AWSCredentialsProviderProvider.create(dynamicAccess, pluginConfig)

    val builder = AmazonDynamoDBAsyncClientBuilder.standard().withClientConfiguration(cc)

    csmConfigurationProviderProvider.create.foreach { c => builder.setClientSideMonitoringConfigurationProvider(c) }
    monitoringListenerProvider.create.foreach { m => builder.setMonitoringListener(m) }
    builder.setRequestHandlers(requestHandlersProvider.create: _*)
    requestMetricCollectorProvider.create.foreach { r => builder.setMetricsCollector(r) }

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

}
