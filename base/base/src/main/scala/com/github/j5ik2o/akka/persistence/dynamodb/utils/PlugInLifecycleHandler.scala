/*
 * Copyright 2023 Junichi Kato
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

import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext

trait PlugInLifecycleHandler {
  def start(): Unit
  def stop(): Unit
}

object PlugInLifecycleHandler {
  final class Default extends PlugInLifecycleHandler {
    override def start(): Unit = {}
    override def stop(): Unit  = {}
  }
}

trait PlugInLifecycleHandlerFactory {
  def create: PlugInLifecycleHandler
}

object PlugInLifecycleHandlerFactory {
  def create(pluginContext: PluginContext): PlugInLifecycleHandlerFactory = {
    val className = pluginContext.pluginConfig.plugInLifecycleHandlerFactoryClassName
    pluginContext.newDynamicAccessor[PlugInLifecycleHandlerFactory]().createThrow(className)
  }

  final class Default(pluginContext: PluginContext) extends PlugInLifecycleHandlerFactory {
    override def create: PlugInLifecycleHandler = {
      val className = pluginContext.pluginConfig.plugInLifecycleHandlerClassName
      pluginContext.newDynamicAccessor[PlugInLifecycleHandler]().createThrow(className)
    }
  }
}
