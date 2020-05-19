package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.amazonaws.DnsResolver
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

import scala.collection.immutable._

trait DnsResolverProvider {
  def create: Option[DnsResolver]
}

object DnsResolverProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): DnsResolverProvider = {
    val className = pluginConfig.clientConfig.v1ClientConfig.clientConfiguration.dnsResolverProviderClassName
    dynamicAccess
      .createInstanceFor[DnsResolverProvider](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ).getOrElse(throw new ClassNotFoundException(className))
  }
}

class DnsResolverProviderImpl(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends DnsResolverProvider {

  override def create: Option[DnsResolver] = {
    val classNameOpt = pluginConfig.clientConfig.v1ClientConfig.clientConfiguration.dnsResolverClassName
    classNameOpt.map { className =>
      dynamicAccess
        .createInstanceFor[DnsResolver](
          className,
          Seq(
            classOf[PluginConfig] -> pluginConfig
          )
        ).getOrElse(throw new ClassNotFoundException(className))
    }
  }
}
