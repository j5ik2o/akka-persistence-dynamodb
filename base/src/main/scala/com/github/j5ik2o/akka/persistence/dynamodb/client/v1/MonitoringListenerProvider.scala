package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.amazonaws.monitoring.MonitoringListener
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException

import scala.collection.immutable._
import scala.util.{ Failure, Success }

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
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize MonitoringListenerProvider", Some(ex))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends MonitoringListenerProvider {

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
          ) match {
          case Success(value) => value
          case Failure(ex) =>
            throw new PluginException("Failed to initialize MonitoringListener", Some(ex))
        }
      }
    }
  }

}
