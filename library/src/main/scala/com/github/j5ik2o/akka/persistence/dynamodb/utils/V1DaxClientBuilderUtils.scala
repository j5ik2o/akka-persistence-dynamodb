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

import com.amazon.dax.client.dynamodbv2.{ AmazonDaxAsyncClientBuilder, AmazonDaxClientBuilder }
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

object V1DaxClientBuilderUtils {

  def setupSync(pluginConfig: PluginConfig): AmazonDynamoDB = {
    val cc      = V1DaxClientConfigUtils.setup(pluginConfig.clientConfig)
    val builder = AmazonDaxClientBuilder.standard().withClientConfiguration(cc)
    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.setCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
        )
      case _ =>
    }
    pluginConfig.clientConfig.region.foreach(builder.setRegion)
    pluginConfig.clientConfig.endpoint.foreach { v => builder.setEndpointConfiguration(v.split(","): _*) }
    builder.build()
  }

  def setupAsync(pluginConfig: PluginConfig): AmazonDynamoDBAsync = {
    val cc      = V1DaxClientConfigUtils.setup(pluginConfig.clientConfig)
    val builder = AmazonDaxAsyncClientBuilder.standard().withClientConfiguration(cc)
    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.setCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
        )
      case _ =>
    }
    pluginConfig.clientConfig.region.foreach(builder.setRegion)
    pluginConfig.clientConfig.endpoint.foreach { v => builder.setEndpointConfiguration(v.split(","): _*) }
    builder.build()
  }

}
