package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import akka.event.LoggingAdapter
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

object V2ClientUtils {

  def createV2SyncClient(
      dynamicAccess: DynamicAccess,
      configRootPath: String,
      pluginConfig: PluginConfig
  )(implicit
      log: LoggingAdapter
  ): JavaDynamoDbSyncClient = {
    if (pluginConfig.clientConfig.v2ClientConfig.dispatcherName.isEmpty)
      log.warning(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v2.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    val javaSyncClientV2 = V2DynamoDbClientBuilderUtils
      .setupSync(
        dynamicAccess,
        pluginConfig
      ).build()
    javaSyncClientV2
  }

  def createV2AsyncClient(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): JavaDynamoDbAsyncClient = {
    val javaAsyncClientV2 = V2DynamoDbClientBuilderUtils
      .setupAsync(
        dynamicAccess,
        pluginConfig
      ).build()
    javaAsyncClientV2
  }

}
