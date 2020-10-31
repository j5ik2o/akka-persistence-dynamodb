package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.time.{ Duration => JavaDuration }

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.ExecutionInterceptorsProvider
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.{ MetricPublishersProvider, RetryPolicyProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

object V2ClientOverrideConfigurationBuilderUtils {

  def setup(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): ClientOverrideConfiguration.Builder = {
    import pluginConfig.clientConfig.v2ClientConfig._
    var clientOverrideConfigurationBuilder = ClientOverrideConfiguration
      .builder()
    headers.map {
      case (k, v) =>
        clientOverrideConfigurationBuilder.putHeader(k, v.asJava)
    }
    retryMode.foreach { v => clientOverrideConfigurationBuilder = clientOverrideConfigurationBuilder.retryPolicy(v) }
    RetryPolicyProvider.create(dynamicAccess, pluginConfig).foreach { rp =>
      clientOverrideConfigurationBuilder = clientOverrideConfigurationBuilder.retryPolicy(rp.create)
    }
    val provider = ExecutionInterceptorsProvider.create(dynamicAccess, pluginConfig)
    provider.create.foreach { ei =>
      clientOverrideConfigurationBuilder = clientOverrideConfigurationBuilder.addExecutionInterceptor(ei)
    }
    // putAdvancedOption
    apiCallTimeout.foreach { v =>
      if (v != Duration.Zero)
        clientOverrideConfigurationBuilder =
          clientOverrideConfigurationBuilder.apiCallTimeout(JavaDuration.ofMillis(v.toMillis))
    }
    apiCallAttemptTimeout.foreach { v =>
      if (v != Duration.Zero)
        clientOverrideConfigurationBuilder =
          clientOverrideConfigurationBuilder.apiCallAttemptTimeout(JavaDuration.ofMillis(v.toMillis))
    }
    // defaultProfileFile
    // defaultProfileName
    val metricPublishersProvider = MetricPublishersProvider.create(dynamicAccess, pluginConfig)
    val metricPublishers         = metricPublishersProvider.create
    clientOverrideConfigurationBuilder.metricPublishers(metricPublishers.asJava)
    clientOverrideConfigurationBuilder
  }
}
