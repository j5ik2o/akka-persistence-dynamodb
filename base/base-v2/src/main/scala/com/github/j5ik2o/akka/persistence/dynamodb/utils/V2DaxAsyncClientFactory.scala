package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

trait V2DaxAsyncClientFactory {
  def create: JavaDynamoDbAsyncClient
}

object V2DaxAsyncClientFactory {

  class Default(pluginContext: PluginContext) extends V2DaxAsyncClientFactory {
    override def create: JavaDynamoDbAsyncClient = {
      V2ClientUtils.createV2DaxAsyncClient(pluginContext)
    }
  }

}
