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

import akka.actor.DynamicAccess
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

object V1ClientUtils extends LoggingSupport {

  def createV1AsyncClient(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): AmazonDynamoDBAsync = {
    V1ClientBuilderUtils.setupAsync(dynamicAccess, pluginConfig).build()
  }

  def createV1SyncClient(
      dynamicAccess: DynamicAccess,
      configRootPath: String,
      pluginConfig: PluginConfig
  ): AmazonDynamoDB = {
    if (pluginConfig.clientConfig.v1ClientConfig.dispatcherName.isEmpty)
      logger.warn(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v1.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    V1ClientBuilderUtils.setupSync(dynamicAccess, pluginConfig).build()
  }

  def createV1DaxSyncClient(
      dynamicAccess: DynamicAccess,
      configRootPath: String,
      pluginConfig: PluginConfig
  ): AmazonDynamoDB = {
    if (pluginConfig.clientConfig.v1DaxClientConfig.dispatcherName.isEmpty)
      logger.warn(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v1-dax.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    V1DaxClientBuilderUtils.setupSync(dynamicAccess, pluginConfig).build()
  }

  def createV1DaxAsyncClient(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDBAsync = {
    V1DaxClientBuilderUtils.setupAsync(dynamicAccess, pluginConfig).build()
  }
}
