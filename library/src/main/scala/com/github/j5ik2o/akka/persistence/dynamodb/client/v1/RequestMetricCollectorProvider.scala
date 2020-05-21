package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.amazonaws.metrics.RequestMetricCollector
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

import scala.collection.immutable._

trait RequestMetricCollectorProvider {
  def create: Option[RequestMetricCollector]
}

object RequestMetricCollectorProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): RequestMetricCollectorProvider = {
    val className = pluginConfig.clientConfig.v1ClientConfig.requestMetricCollectorProviderClassName
    dynamicAccess
      .createInstanceFor[RequestMetricCollectorProvider](
        className,
        Seq(classOf[DynamicAccess] -> dynamicAccess, classOf[PluginConfig] -> pluginConfig)
      ).getOrElse(
        throw new ClassNotFoundException(className)
      )
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends RequestMetricCollectorProvider {

    override def create: Option[RequestMetricCollector] = {
      val classNameOpt = pluginConfig.clientConfig.v1ClientConfig.requestMetricCollectorClassName
      classNameOpt.map { className =>
        dynamicAccess
          .createInstanceFor[RequestMetricCollector](
            className,
            Seq(classOf[DynamicAccess] -> dynamicAccess, classOf[PluginConfig] -> pluginConfig)
          ).getOrElse(
            throw new ClassNotFoundException(className)
          )
      }
    }
  }

}
