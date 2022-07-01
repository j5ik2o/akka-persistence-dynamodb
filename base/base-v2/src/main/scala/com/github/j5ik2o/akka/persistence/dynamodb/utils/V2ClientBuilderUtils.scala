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

import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
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

object V2ClientBuilderUtils {

  def setupSync(
      pluginContext: PluginContext
  ): DynamoDbClientBuilder = {
    import pluginContext._
    val httpClient: SdkHttpClient = V2HttpClientBuilderUtils.setupSync(pluginConfig).build()
    val clientOverrideConfiguration: ClientOverrideConfiguration =
      V2ClientOverrideConfigurationBuilderUtils.setup(pluginContext).build()

    val builder = DynamoDbClient.builder().httpClient(httpClient).overrideConfiguration(clientOverrideConfiguration)

    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
        val awsCredentialsProviderProvider = AwsCredentialsProviderProvider.create(pluginContext)
        awsCredentialsProviderProvider.create.foreach { cp =>
          builder.credentialsProvider(cp)
        }
    }
    pluginConfig.clientConfig.endpoint.foreach { ep =>
      builder.endpointOverride(URI.create(ep))
    }
    pluginConfig.clientConfig.region.foreach { r =>
      builder.region(Region.of(r))
    }
    builder
  }

  def setupAsync(pluginContext: PluginContext): DynamoDbAsyncClientBuilder = {
    import pluginContext._
    val httpClient: SdkAsyncHttpClient = V2HttpClientBuilderUtils.setupAsync(pluginConfig).build()
    val clientOverrideConfiguration: ClientOverrideConfiguration = V2ClientOverrideConfigurationBuilderUtils
      .setup(pluginContext)
      .build()

    val builder =
      DynamoDbAsyncClient.builder().httpClient(httpClient).overrideConfiguration(clientOverrideConfiguration)

    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
        val awsCredentialsProviderProvider = AwsCredentialsProviderProvider.create(pluginContext)
        awsCredentialsProviderProvider.create.foreach { cp =>
          builder.credentialsProvider(cp)
        }
    }
    pluginConfig.clientConfig.endpoint.foreach { ep =>
      builder.endpointOverride(URI.create(ep))
    }
    pluginConfig.clientConfig.region.foreach { r =>
      builder.region(Region.of(r))
    }
    builder
  }

}
