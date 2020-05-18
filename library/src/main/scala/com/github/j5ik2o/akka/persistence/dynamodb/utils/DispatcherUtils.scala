package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Flow
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion

object DispatcherUtils {

  def applyV1Dispatcher[A, B](pluginConfig: PluginConfig, flow: Flow[A, B, NotUsed]): Flow[A, B, NotUsed] = {
    (if (pluginConfig.clientConfig.clientVersion == ClientVersion.V1)
       pluginConfig.clientConfig.v1ClientConfig.dispatcherName
     else
       pluginConfig.clientConfig.v1DaxClientConfig.dispatcherName)
      .fold(flow) { name => flow.withAttributes(ActorAttributes.dispatcher(name)) }
  }

  def applyV2Dispatcher[A, B](pluginConfig: PluginConfig, flow: Flow[A, B, NotUsed]): Flow[A, B, NotUsed] = {
    pluginConfig.clientConfig.v2ClientConfig.dispatcherName.fold(flow) { name =>
      flow.withAttributes(ActorAttributes.dispatcher(name))
    }
  }

}
