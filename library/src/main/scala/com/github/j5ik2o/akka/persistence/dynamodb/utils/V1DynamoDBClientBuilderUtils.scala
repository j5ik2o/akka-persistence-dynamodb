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
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBAsync,
  AmazonDynamoDBAsyncClientBuilder,
  AmazonDynamoDBClientBuilder
}
import com.github.j5ik2o.akka.persistence.dynamodb.config.DynamoDBClientConfig

object V1DynamoDBClientBuilderUtils {

  def setupSync(dynamicAccess: DynamicAccess, clientConfig: DynamoDBClientConfig): AmazonDynamoDB = {
    val cc      = clientConfig.v1ClientConfig.toAWS(dynamicAccess)
    val builder = AmazonDynamoDBClientBuilder.standard().withClientConfiguration(cc)
    (clientConfig.accessKeyId, clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.setCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
        )
      case _ =>
    }
    (clientConfig.region, clientConfig.endpoint) match {
      case (Some(r), Some(e)) =>
        builder.setEndpointConfiguration(new EndpointConfiguration(e, r))
      case (Some(r), _) =>
        builder.setRegion(r)
      case _ =>
    }
    builder.build()
  }

  def setupAsync(dynamicAccess: DynamicAccess, clientConfig: DynamoDBClientConfig): AmazonDynamoDBAsync = {
    val cc      = clientConfig.v1ClientConfig.toAWS(dynamicAccess)
    val builder = AmazonDynamoDBAsyncClientBuilder.standard().withClientConfiguration(cc)
    (clientConfig.accessKeyId, clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.setCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
        )
      case _ =>
    }
    (clientConfig.region, clientConfig.endpoint) match {
      case (Some(r), Some(e)) =>
        builder.setEndpointConfiguration(new EndpointConfiguration(e, r))
      case (Some(r), _) =>
        builder.setRegion(r)
      case _ =>
    }
    builder.build()
  }

}
