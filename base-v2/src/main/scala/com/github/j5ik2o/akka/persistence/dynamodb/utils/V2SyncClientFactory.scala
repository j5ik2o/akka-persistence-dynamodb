package com.github.j5ik2o.akka.persistence.dynamodb.utils
import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.services.dynamodb.{ DynamoDbClient => JavaDynamoDbSyncClient }

trait V2SyncClientFactory {
  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): JavaDynamoDbSyncClient
}

object V2SyncClientFactory {

  class Default extends V2SyncClientFactory {
    override def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): JavaDynamoDbSyncClient = {
      V2ClientUtils.createV2SyncClient(dynamicAccess, pluginConfig.configRootPath, pluginConfig)
    }
  }
}
