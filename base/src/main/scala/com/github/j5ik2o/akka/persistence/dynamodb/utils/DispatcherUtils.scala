package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Flow
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion

object DispatcherUtils extends LoggingSupport {

  def applyV1Dispatcher[A, B](pluginConfig: PluginConfig, flow: Flow[A, B, NotUsed]): Flow[A, B, NotUsed] = {
    val dispatcherName = pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V1 =>
        pluginConfig.clientConfig.v1ClientConfig.dispatcherName
      case ClientVersion.V1Dax =>
        pluginConfig.clientConfig.v1DaxClientConfig.dispatcherName
      case _ =>
        throw new IllegalArgumentException("Invalid the client version")
    }
    dispatcherName.fold(
      flow
    ) { name => flow.withAttributes(ActorAttributes.dispatcher(name)) }
  }

  def applyV2Dispatcher[A, B](pluginConfig: PluginConfig, flow: Flow[A, B, NotUsed]): Flow[A, B, NotUsed] = {
    val dispatcherName = pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V2 =>
        pluginConfig.clientConfig.v2ClientConfig.dispatcherName
      case _ =>
        throw new IllegalArgumentException("Invalid the client version")
    }
    dispatcherName.fold(
      flow
    ) { name => flow.withAttributes(ActorAttributes.dispatcher(name)) }
  }

}
