package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import java.security.SecureRandom

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

import scala.collection.immutable._

trait SecureRandomProvider {
  def create: SecureRandom
}

object SecureRandomProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): SecureRandomProvider = {
    val className = pluginConfig.clientConfig.v1ClientConfig.clientConfiguration.secureRandomProviderClassName
    dynamicAccess
      .createInstanceFor[SecureRandomProvider](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ).getOrElse(throw new ClassNotFoundException(className))
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends SecureRandomProvider {
    override def create: SecureRandom = new SecureRandom()
  }
}
