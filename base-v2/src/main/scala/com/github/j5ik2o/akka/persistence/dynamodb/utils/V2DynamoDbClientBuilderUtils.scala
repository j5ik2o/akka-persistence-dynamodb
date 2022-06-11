package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient,
  DynamoDbAsyncClientBuilder,
  DynamoDbClient,
  DynamoDbClientBuilder
}

import java.net.URI

object V2DynamoDbClientBuilderUtils {

  def setupSync(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): DynamoDbClientBuilder = {
    val httpClient: SdkHttpClient = V2HttpClientBuilderUtils.setupSync(pluginConfig).build()
    val clientOverrideConfiguration: ClientOverrideConfiguration =
      V2ClientOverrideConfigurationBuilderUtils.setup(dynamicAccess, pluginConfig).build()
    val awsCredentialsProviderProvider = AwsCredentialsProviderProvider.create(dynamicAccess, pluginConfig)

    var builder =
      DynamoDbClient
        .builder().httpClient(httpClient).overrideConfiguration(clientOverrideConfiguration)
    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder = builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
        awsCredentialsProviderProvider.create.foreach { cp => builder = builder.credentialsProvider(cp) }
    }
    pluginConfig.clientConfig.endpoint.foreach { ep => builder = builder.endpointOverride(URI.create(ep)) }
    pluginConfig.clientConfig.region.foreach { r => builder = builder.region(Region.of(r)) }
    builder
  }

  def setupAsync(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): DynamoDbAsyncClientBuilder = {
    val httpClient: SdkAsyncHttpClient = V2HttpClientBuilderUtils.setupAsync(pluginConfig).build()
    val clientOverrideConfiguration: ClientOverrideConfiguration =
      V2ClientOverrideConfigurationBuilderUtils.setup(dynamicAccess, pluginConfig).build()
    val awsCredentialsProviderProvider = AwsCredentialsProviderProvider.create(dynamicAccess, pluginConfig)

    var builder =
      DynamoDbAsyncClient
        .builder().httpClient(httpClient).overrideConfiguration(clientOverrideConfiguration)
    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder = builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
        awsCredentialsProviderProvider.create.foreach { cp => builder = builder.credentialsProvider(cp) }
    }
    pluginConfig.clientConfig.endpoint.foreach { ep => builder = builder.endpointOverride(URI.create(ep)) }
    pluginConfig.clientConfig.region.foreach { r => builder = builder.region(Region.of(r)) }
    builder
  }

}
