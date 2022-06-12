package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

trait V2AsyncClientFactory {
  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): JavaDynamoDbAsyncClient
}

object V2AsyncClientFactory {

  class Default extends V2AsyncClientFactory {
    override def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): JavaDynamoDbAsyncClient = {
      V2ClientUtils.createV2AsyncClient(dynamicAccess, pluginConfig)
    }
  }

}
