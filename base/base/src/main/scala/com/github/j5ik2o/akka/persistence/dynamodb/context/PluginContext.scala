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
package com.github.j5ik2o.akka.persistence.dynamodb.context

import akka.actor.{ ActorSystem, DynamicAccess, ExtendedActorSystem }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessor

import scala.reflect.ClassTag

trait PluginContext {
  type This <: PluginContext
  val pluginConfig: PluginConfig
  def system: ActorSystem
  def dynamicAccess: DynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess

  def newDynamicAccessor[A: ClassTag](): DynamicAccessor[A, This]
}
