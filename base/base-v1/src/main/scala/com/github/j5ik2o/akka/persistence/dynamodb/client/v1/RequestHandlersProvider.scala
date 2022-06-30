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
package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import com.amazonaws.handlers.RequestHandler2
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext

import scala.collection.immutable._

trait RequestHandlersProvider {
  def create: Seq[RequestHandler2]
}

object RequestHandlersProvider {

  def create(pluginContext: PluginContext): RequestHandlersProvider = {
    val className = pluginContext.pluginConfig.clientConfig.v1ClientConfig.requestHandlersProviderClassName
    pluginContext.newDynamicAccessor[RequestHandlersProvider]().createThrow(className)
  }

  final class Default(pluginContext: PluginContext) extends RequestHandlersProvider {

    override def create: Seq[RequestHandler2] = {
      val classNames = pluginContext.pluginConfig.clientConfig.v1ClientConfig.requestHandlerClassNames
      classNames.map { className =>
        pluginContext.newDynamicAccessor[RequestHandler2]().createThrow(className)
      }
    }
  }

}
