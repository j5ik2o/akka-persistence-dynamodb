package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

trait V1SyncClientFactory {
  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDB
}

object V1SyncClientFactory {

  class Default extends V1SyncClientFactory {
    override def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDB = {
      V1ClientUtils.createV1SyncClient(dynamicAccess, pluginConfig.configRootPath, pluginConfig)
    }
  }
}
