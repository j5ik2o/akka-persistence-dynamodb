/*
 * Copyright 2019 Junichi Kato
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

import java.net.URI

import com.github.j5ik2o.akka.persistence.dynamodb.config.DynamoDBClientConfig
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

object V2DynamoDbClientBuilderUtils {

  def setupSync(
      clientConfig: DynamoDBClientConfig,
      httpClient: SdkHttpClient,
      clientOverrideConfiguration: ClientOverrideConfiguration
  ): DynamoDbClient = {
    var builder =
      DynamoDbClient
        .builder().httpClient(httpClient).overrideConfiguration(clientOverrideConfiguration)
    (clientConfig.accessKeyId, clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder = builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
    }
    clientConfig.endpoint.foreach { ep => builder = builder.endpointOverride(URI.create(ep)) }
    clientConfig.region.foreach { r => builder = builder.region(Region.of(r)) }
    builder.build()
  }

  def setupAsync(
      clientConfig: DynamoDBClientConfig,
      httpClient: SdkAsyncHttpClient,
      clientOverrideConfiguration: ClientOverrideConfiguration
  ): DynamoDbAsyncClient = {
    var builder =
      DynamoDbAsyncClient
        .builder().httpClient(httpClient).overrideConfiguration(clientOverrideConfiguration)
    (clientConfig.accessKeyId, clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder = builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
    }
    clientConfig.endpoint.foreach { ep => builder = builder.endpointOverride(URI.create(ep)) }
    clientConfig.region.foreach { r => builder = builder.region(Region.of(r)) }
    builder.build()
  }

}
