package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.amazonaws.retry.{ PredefinedRetryPolicies, RetryPolicy }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

import scala.collection.immutable._

trait RetryPolicyProvider {
  def create: RetryPolicy
}

object RetryPolicyProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): Option[RetryPolicyProvider] = {
    val classNameOpt = pluginConfig.clientConfig.v1ClientConfig.clientConfiguration.retryPolicyProviderClassName
    classNameOpt.map { className =>
      dynamicAccess
        .createInstanceFor[RetryPolicyProvider](
          className,
          Seq(
            classOf[DynamicAccess] -> dynamicAccess,
            classOf[PluginConfig]  -> pluginConfig
          )
        ).getOrElse(throw new ClassNotFoundException(className))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends RetryPolicyProvider {

    override def create: RetryPolicy = {
      pluginConfig.clientConfig.v1ClientConfig.clientConfiguration.maxErrorRetry
        .fold(PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicy) { maxErrorRetry =>
          PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(maxErrorRetry)
        }
    }

  }

}
