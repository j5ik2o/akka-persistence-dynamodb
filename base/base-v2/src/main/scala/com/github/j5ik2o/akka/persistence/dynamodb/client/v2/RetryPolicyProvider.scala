/*
 * Copyright 2020 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import software.amazon.awssdk.core.retry.RetryPolicy

trait RetryPolicyProvider {
  def create: RetryPolicy
}

object RetryPolicyProvider {

  def create(pluginContext: PluginContext): RetryPolicyProvider = {
    val className = pluginContext.pluginConfig.clientConfig.v2ClientConfig.retryPolicyProviderClassName
    pluginContext.newDynamicAccessor[RetryPolicyProvider]().createThrow(className)
  }

  final class Default extends RetryPolicyProvider {

    override def create: RetryPolicy = {
      RetryPolicy.defaultRetryPolicy()
    }
  }

}
