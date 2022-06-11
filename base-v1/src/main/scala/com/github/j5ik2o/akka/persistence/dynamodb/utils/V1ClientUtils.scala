package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import akka.event.LoggingAdapter
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync}
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

object V1ClientUtils {

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
                        )(implicit
                          log: LoggingAdapter
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
