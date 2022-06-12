package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

private[utils] object V1ClientUtils extends LoggingSupport {

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
  ): AmazonDynamoDB = {
    if (pluginConfig.clientConfig.v1ClientConfig.dispatcherName.isEmpty)
      logger.warn(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v1.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    V1DynamoDBClientBuilderUtils.setupSync(dynamicAccess, pluginConfig).build()
  }

  def createV1DaxSyncClient(
      configRootPath: String,
      dynamoDBClientConfig: DynamoDBClientConfig
  ): AmazonDynamoDB = {
    if (dynamoDBClientConfig.v1DaxClientConfig.dispatcherName.isEmpty)
      logger.warn(
        s"Please set a dispatcher name defined by you to `${configRootPath}.dynamo-db-client.v1-dax.dispatcher-name` if you are using the AWS-SDK API for blocking I/O"
      )
    V1DaxClientBuilderUtils.setupSync(dynamoDBClientConfig).build()
  }

  def createV1DaxAsyncClient(dynamoDBClientConfig: DynamoDBClientConfig): AmazonDynamoDBAsync = {
    V1DaxClientBuilderUtils.setupAsync(dynamoDBClientConfig).build()
  }
}
