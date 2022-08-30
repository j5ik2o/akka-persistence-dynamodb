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
import net.ceedubs.ficus.Ficus._

final case class TableName(private val value: String) {
  def asString: String = value
}

trait TableNameResolver {
  def resolve(persistenceId: PersistenceId): TableName
}

trait TableNameResolverProvider {

  def create: TableNameResolver

}

object TableNameResolverProvider {

  def create(pluginContext: StatePluginContext): TableNameResolverProvider = {
    val className = pluginContext.pluginConfig.tableNameResolverProviderClassName
    StateDynamicAccessor[TableNameResolverProvider](pluginContext).createThrow(className)
  }

  final class Default(pluginContext: StatePluginContext) extends TableNameResolverProvider {

    override def create: TableNameResolver = {
      val className = pluginContext.pluginConfig.tableNameResolverClassName
      StateDynamicAccessor[TableNameResolver](pluginContext).createThrow(className)
    }

  }
}

object TableNameResolver {

  final class Config(pluginContext: StatePluginContext) extends TableNameResolver {
    override def resolve(persistenceId: PersistenceId): TableName = TableName(pluginContext.pluginConfig.tableName)
  }

  final class Prefix(pluginContext: StatePluginContext) extends TableNameResolver with ToPersistenceIdOps {
    import pluginContext._

    override def separator: String =
      pluginConfig.sourceConfig.getAs[String]("persistence-id-separator").getOrElse(PersistenceId.Separator)

    override def resolve(persistenceId: PersistenceId): TableName = {
      TableName(persistenceId.prefix.getOrElse(pluginConfig.tableName))
    }

  }

}
