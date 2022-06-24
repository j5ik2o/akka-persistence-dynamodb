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
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessUtils
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor

trait ExecutionInterceptorsProvider {
  def create: Seq[ExecutionInterceptor]
}

object ExecutionInterceptorsProvider {

  def create(pluginContext: PluginContext): ExecutionInterceptorsProvider = {
    val className = pluginContext.pluginConfig.clientConfig.v2ClientConfig.executionInterceptorsProviderClassName
    DynamicAccessUtils.createInstanceFor_CTX_Throw[ExecutionInterceptorsProvider, PluginContext](
      className,
      pluginContext
    )
  }

  final class Default(pluginContext: PluginContext) extends ExecutionInterceptorsProvider {

    override def create: Seq[ExecutionInterceptor] = {
      val classNames = pluginContext.pluginConfig.clientConfig.v2ClientConfig.executionInterceptorClassNames
      classNames.map { className =>
        DynamicAccessUtils.createInstanceFor_CTX_Throw[ExecutionInterceptor, PluginContext](className, pluginContext)
      }
    }
  }

}
