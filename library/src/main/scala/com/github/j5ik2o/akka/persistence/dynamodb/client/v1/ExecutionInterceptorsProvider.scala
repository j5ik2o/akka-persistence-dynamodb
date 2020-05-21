package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor

import scala.collection.immutable._

trait ExecutionInterceptorsProvider {
  def create: Seq[ExecutionInterceptor]
}

object ExecutionInterceptorsProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): ExecutionInterceptorsProvider = {
    val className = pluginConfig.clientConfig.v2ClientConfig.executionInterceptorsProviderClassName
    dynamicAccess
      .createInstanceFor[ExecutionInterceptorsProvider](
        className,
        Seq(classOf[DynamicAccess] -> dynamicAccess, classOf[PluginConfig] -> pluginConfig)
      ).getOrElse(throw new ClassNotFoundException(className))
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends ExecutionInterceptorsProvider {

    override def create: Seq[ExecutionInterceptor] = {
      val classNames = pluginConfig.clientConfig.v2ClientConfig.executionInterceptorClassNames
      classNames.map { className =>
        dynamicAccess
          .createInstanceFor[ExecutionInterceptor](className, Seq(classOf[PluginConfig] -> pluginConfig)).getOrElse(
            throw new ClassNotFoundException(className)
          )
      }
    }
  }

}
