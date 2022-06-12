package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

trait V1DaxAsyncClientFactory {
  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDBAsync
}

object V1DaxAsyncClientFactory {
  class Default extends V1DaxAsyncClientFactory {
    override def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDBAsync = {
      V1ClientUtils.createV1DaxAsyncClient(pluginConfig.clientConfig)
    }
  }
}
