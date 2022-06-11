package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.model.PersistenceId
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

case class TableName(private val value: String) {
  def asString: String = value
}

trait TableNameResolver {
  def resolve(persistenceId: PersistenceId): TableName
}

trait TableNameResolverProvider {

  def create: TableNameResolver

}

object TableNameResolverProvider {

  def create(dynamicAccess: DynamicAccess, statePluginConfig: StatePluginConfig): TableNameResolverProvider = {
    val className = statePluginConfig.tableNameResolverProviderClassName
    dynamicAccess
      .createInstanceFor[TableNameResolverProvider](
        className,
        Seq(
          classOf[DynamicAccess]     -> dynamicAccess,
          classOf[StatePluginConfig] -> statePluginConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize PartitionKeyResolverProvider", Some(ex))
    }

  }

  final class Default(dynamicAccess: DynamicAccess, statePluginConfig: StatePluginConfig)
      extends TableNameResolverProvider {

    override def create: TableNameResolver = {
      val className = statePluginConfig.tableNameResolverClassName
      val args =
        Seq(classOf[StatePluginConfig] -> statePluginConfig)
      dynamicAccess
        .createInstanceFor[TableNameResolver](
          className,
          args
        ) match {
        case Success(value) => value
        case Failure(ex) =>
          throw new PluginException("Failed to initialize PartitionKeyResolver", Some(ex))
      }
    }

  }
}

object TableNameResolver {

  class Default(statePluginConfig: StatePluginConfig) extends Config(statePluginConfig)

  class Config(statePluginConfig: StatePluginConfig) extends TableNameResolver {
    override def resolve(persistenceId: PersistenceId): TableName = TableName(statePluginConfig.tableName)
  }

  class Prefix(statePluginConfig: StatePluginConfig) extends TableNameResolver with ToPersistenceIdOps {

    override def separator: String =
      statePluginConfig.sourceConfig.valueAs[String]("persistence-id-separator", PersistenceId.Separator)

    override def resolve(persistenceId: PersistenceId): TableName = {
      TableName(persistenceId.prefix.getOrElse(statePluginConfig.tableName))
    }

  }

}
