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
package com.github.j5ik2o.akka.persistence.dynamodb.state

import com.github.j5ik2o.akka.persistence.dynamodb.model.PersistenceId

final case class SortKey(value: String) {
  def asString: String = value
}

trait SortKeyResolver {
  def resolve(persistenceId: PersistenceId): SortKey
}

trait SortKeyResolverProvider {

  def create: SortKeyResolver

}

object SortKeyResolverProvider {

  def create(pluginContext: StatePluginContext): SortKeyResolverProvider = {
    val className = pluginContext.pluginConfig.sortKeyResolverProviderClassName
    pluginContext.newDynamicAccessor[SortKeyResolverProvider]().createThrow(className)
  }

  final class Default(
      pluginContext: StatePluginContext
  ) extends SortKeyResolverProvider {

    override def create: SortKeyResolver = {
      val className = pluginContext.pluginConfig.sortKeyResolverClassName
      pluginContext.newDynamicAccessor[SortKeyResolver]().createThrow(className)
    }

  }

}

object SortKeyResolver {

  final class PersistenceIdBased extends SortKeyResolver {

    override def resolve(persistenceId: PersistenceId): SortKey = {
      SortKey(persistenceId.asString)
    }

  }

}
