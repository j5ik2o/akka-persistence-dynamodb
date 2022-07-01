/*
 * Copyright 2022 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.{
  ExecutionInterceptorsProvider,
  MetricPublishersProvider,
  RetryPolicyProvider
}
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryMode

import java.time.{ Duration => JavaDuration }
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

object V2ClientOverrideConfigurationBuilderUtils {

  def setup(pluginContext: PluginContext): ClientOverrideConfiguration.Builder = {
    import pluginContext._
    import pluginConfig.clientConfig.v2ClientConfig._
    val clientOverrideConfigurationBuilder = ClientOverrideConfiguration.builder()
    headers.foreach { case (k, v) =>
      clientOverrideConfigurationBuilder.putHeader(k, v.asJava)
    }
    retryMode.foreach { v =>
      val r = v match {
        case com.github.j5ik2o.akka.persistence.dynamodb.config.client.RetryMode.LEGACY =>
          RetryMode.LEGACY
        case com.github.j5ik2o.akka.persistence.dynamodb.config.client.RetryMode.STANDARD =>
          RetryMode.STANDARD
        case com.github.j5ik2o.akka.persistence.dynamodb.config.client.RetryMode.ADAPTIVE =>
          RetryMode.ADAPTIVE
      }
      clientOverrideConfigurationBuilder.retryPolicy(r)
    }
    val rp = RetryPolicyProvider.create(pluginContext)
    clientOverrideConfigurationBuilder.retryPolicy(rp.create)
    val provider = ExecutionInterceptorsProvider.create(pluginContext)
    provider.create.foreach { ei =>
      clientOverrideConfigurationBuilder.addExecutionInterceptor(ei)
    }
    // putAdvancedOption
    apiCallTimeout.foreach { v =>
      if (v != Duration.Zero)
        clientOverrideConfigurationBuilder.apiCallTimeout(
          JavaDuration.ofMillis(v.toMillis)
        )
    }
    apiCallAttemptTimeout.foreach { v =>
      if (v != Duration.Zero)
        clientOverrideConfigurationBuilder.apiCallAttemptTimeout(
          JavaDuration.ofMillis(v.toMillis)
        )
    }
    // defaultProfileFile
    // defaultProfileName
    val metricPublishersProvider = MetricPublishersProvider.create(pluginContext)
    val metricPublishers         = metricPublishersProvider.create
    clientOverrideConfigurationBuilder.metricPublishers(
      metricPublishers.asJava
    )
    clientOverrideConfigurationBuilder
  }
}
