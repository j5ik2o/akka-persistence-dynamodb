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
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  StaticCredentialsProvider,
  WebIdentityTokenFileCredentialsProvider
}
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }

object DynamoDbClientBuilderUtils {

  def setup(clientConfig: DynamoDBClientConfig, httpClientBuilder: SdkAsyncHttpClient): DynamoDbAsyncClientBuilder = {
    var builder = DynamoDbAsyncClient.builder().httpClient(httpClientBuilder)
    (sys.env.get("AWS_ROLE_ARN"), clientConfig.accessKeyId, clientConfig.secretAccessKey) match {
      case (Some(_), _, _) =>
        builder = builder.credentialsProvider(WebIdentityTokenFileCredentialsProvider.create())
      case (None, Some(a), Some(s)) =>
        builder = builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
    }
    clientConfig.endpoint.foreach { ep => builder = builder.endpointOverride(URI.create(ep)) }
    clientConfig.region.foreach { r => builder = builder.region(Region.of(r)) }
    builder
  }

}
