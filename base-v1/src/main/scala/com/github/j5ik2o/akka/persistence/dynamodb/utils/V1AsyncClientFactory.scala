package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

trait V1AsyncClientFactory {
  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDBAsync
}

object V1AsyncClientFactory {
  class Default extends V1AsyncClientFactory {
    override def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AmazonDynamoDBAsync = {
      V1ClientUtils.createV1AsyncClient(dynamicAccess, pluginConfig)
    }
  }
}
