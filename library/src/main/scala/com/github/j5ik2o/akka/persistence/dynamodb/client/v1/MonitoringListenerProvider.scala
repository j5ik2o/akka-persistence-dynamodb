package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.amazonaws.monitoring.MonitoringListener
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

import scala.collection.immutable._

trait MonitoringListenerProvider {
  def create: Option[MonitoringListener]
}

object MonitoringListenerProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): MonitoringListenerProvider = {
    val className = pluginConfig.clientConfig.v1ClientConfig.monitoringListenerProviderClassName
    dynamicAccess
      .createInstanceFor[MonitoringListenerProvider](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ).getOrElse(
        throw new ClassNotFoundException(className)
      )
  }

  class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends MonitoringListenerProvider {

    override def create: Option[MonitoringListener] = {
      val classNameOpt = pluginConfig.clientConfig.v1ClientConfig.monitoringListenerClassName
      classNameOpt.map { className =>
        dynamicAccess
          .createInstanceFor[MonitoringListener](
            className,
            Seq(
              classOf[DynamicAccess] -> dynamicAccess,
              classOf[PluginConfig]  -> pluginConfig
            )
          ).getOrElse(
            throw new ClassNotFoundException(className)
          )
      }
    }
  }

}
