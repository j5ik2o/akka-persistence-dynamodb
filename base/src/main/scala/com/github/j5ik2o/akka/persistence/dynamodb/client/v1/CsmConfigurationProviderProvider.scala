package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.amazonaws.monitoring.CsmConfigurationProvider
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

trait CsmConfigurationProviderProvider {
  def create: Option[CsmConfigurationProvider]
}

object CsmConfigurationProviderProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): CsmConfigurationProviderProvider = {
    val className = pluginConfig.clientConfig.v1ClientConfig.csmConfigurationProviderProviderClassName
    dynamicAccess
      .createInstanceFor[CsmConfigurationProviderProvider](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize CsmConfigurationProviderProvider", Some(ex))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig)
      extends CsmConfigurationProviderProvider {

    override def create: Option[CsmConfigurationProvider] = {
      val classNameOpt = pluginConfig.clientConfig.v1ClientConfig.monitoringListenerClassName
      classNameOpt.map { className =>
        dynamicAccess
          .createInstanceFor[CsmConfigurationProvider](
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
