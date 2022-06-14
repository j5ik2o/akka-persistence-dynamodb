package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

trait V2DaxAsyncClientFactory {
  def create(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): JavaDynamoDbAsyncClient
}

object V2DaxAsyncClientFactory {

  class Default extends V2DaxAsyncClientFactory {
    override def create(
        dynamicAccess: DynamicAccess,
        pluginConfig: PluginConfig
    ): JavaDynamoDbAsyncClient = {
      V2ClientUtils.createV2DaxAsyncClient(dynamicAccess, pluginConfig)
    }
  }

}
