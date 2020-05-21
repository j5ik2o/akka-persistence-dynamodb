package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.amazonaws.handlers.RequestHandler2
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

import scala.collection.immutable._

trait RequestHandlersProvider {
  def create: Seq[RequestHandler2]
}

object RequestHandlersProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): RequestHandlersProvider = {
    val className = pluginConfig.clientConfig.v1ClientConfig.requestHandlersProviderClassName
    dynamicAccess
      .createInstanceFor[RequestHandlersProvider](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ).getOrElse(throw new ClassNotFoundException(className))
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends RequestHandlersProvider {

    override def create: Seq[RequestHandler2] = {
      val classNames = pluginConfig.clientConfig.v1ClientConfig.requestHandlerClassNames
      classNames.map { className =>
        dynamicAccess
          .createInstanceFor[RequestHandler2](
            className,
            Seq(
              classOf[DynamicAccess] -> dynamicAccess,
              classOf[PluginConfig]  -> pluginConfig
            )
          ).getOrElse(throw new ClassNotFoundException(className))
      }
    }
  }

}
