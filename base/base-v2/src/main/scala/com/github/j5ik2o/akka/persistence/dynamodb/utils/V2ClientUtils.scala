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

import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

object V2ClientUtils extends LoggingSupport {

  def createV2SyncClient(
      pluginContext: PluginContext,
      configRootPath: String
  ): JavaDynamoDbSyncClient = {
    if (pluginContext.pluginConfig.clientConfig.v2ClientConfig.dispatcherName.isEmpty)
      logger.warn(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v2.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    val javaSyncClientV2 = V2ClientBuilderUtils
      .setupSync(pluginContext)
      .build()
    javaSyncClientV2
  }

  def createV2AsyncClient(
      pluginContext: PluginContext
  ): JavaDynamoDbAsyncClient = {
    val javaAsyncClientV2 = V2ClientBuilderUtils
      .setupAsync(pluginContext)
      .build()
    javaAsyncClientV2
  }

  def createV2DaxSyncClient(
      pluginContext: PluginContext,
      configRootPath: String
  ): JavaDynamoDbSyncClient = {
    if (pluginContext.pluginConfig.clientConfig.v2DaxClientConfig.dispatcherName.isEmpty)
      logger.warn(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v2-dax.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    val javaSyncClientV2 = V2DaxClientBuilderUtils
      .setupSync(
        pluginContext
      )
      .build()
    javaSyncClientV2
  }

  def createV2DaxAsyncClient(
      pluginContext: PluginContext
  ): JavaDynamoDbAsyncClient = {
    val javaAsyncClientV2 = V2DaxClientBuilderUtils
      .setupAsync(pluginContext)
      .build()
    javaAsyncClientV2
  }

}
