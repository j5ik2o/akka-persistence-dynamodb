package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import akka.event.LoggingAdapter
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

object ClientUtils {

  def createV2SyncClient(
      dynamicAccess: DynamicAccess,
      configRootPath: String,
      pluginConfig: PluginConfig
  )(f: JavaDynamoDbSyncClient => Unit)(
      implicit log: LoggingAdapter
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
    f(javaSyncClientV2)
    javaSyncClientV2
  }

  def createV2AsyncClient(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  )(f: JavaDynamoDbAsyncClient => Unit): JavaDynamoDbAsyncClient = {
    val javaAsyncClientV2 = V2DynamoDbClientBuilderUtils
      .setupAsync(
        dynamicAccess,
        pluginConfig
      ).build()
    f(javaAsyncClientV2)
    javaAsyncClientV2
  }

  def createV1AsyncClient(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): AmazonDynamoDBAsync = {
    V1DynamoDBClientBuilderUtils.setupAsync(dynamicAccess, pluginConfig).build()
  }

  def createV1SyncClient(
      dynamicAccess: DynamicAccess,
      configRootPath: String,
      pluginConfig: PluginConfig
  )(
      implicit log: LoggingAdapter
  ): AmazonDynamoDB = {
    if (pluginConfig.clientConfig.v1ClientConfig.dispatcherName.isEmpty)
      log.warning(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v1.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    V1DynamoDBClientBuilderUtils.setupSync(dynamicAccess, pluginConfig).build()
  }

  def createV1DaxSyncClient(
      configRootPath: String,
      dynamoDBClientConfig: DynamoDBClientConfig
  )(implicit log: LoggingAdapter): AmazonDynamoDB = {
    if (dynamoDBClientConfig.v1DaxClientConfig.dispatcherName.isEmpty)
      log.warning(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v1-dax.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    V1DaxClientBuilderUtils.setupSync(dynamoDBClientConfig).build()
  }

  def createV1DaxAsyncClient(dynamoDBClientConfig: DynamoDBClientConfig): AmazonDynamoDBAsync = {
    V1DaxClientBuilderUtils.setupAsync(dynamoDBClientConfig).build()
  }

}
