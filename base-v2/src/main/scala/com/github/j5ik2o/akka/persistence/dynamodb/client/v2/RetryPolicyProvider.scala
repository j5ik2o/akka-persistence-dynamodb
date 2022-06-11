package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.core.retry.RetryPolicy

import scala.annotation.unused
import scala.collection.immutable.Seq

trait RetryPolicyProvider {
  def create: RetryPolicy
}

object RetryPolicyProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): Option[RetryPolicyProvider] = {
    val classNameOpt = pluginConfig.clientConfig.v2ClientConfig.retryPolicyProviderClassName
    classNameOpt.map { className =>
      dynamicAccess
        .createInstanceFor[RetryPolicyProvider](
          className,
          Seq(classOf[DynamicAccess] -> dynamicAccess, classOf[PluginConfig] -> pluginConfig)
        ).get
    }
  }

  final class Default(@unused dynamicAccess: DynamicAccess, @unused pluginConfig: PluginConfig)
      extends RetryPolicyProvider {

    override def create: RetryPolicy = {
      RetryPolicy.defaultRetryPolicy()
    }
  }

}
