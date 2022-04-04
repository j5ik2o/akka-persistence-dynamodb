package com.github.j5ik2o.akka.persistence.dynamodb.model

import java.util.UUID

trait Context {
  def id: UUID

  def persistenceId: PersistenceId

  def data: Option[Any]

  def withData(value: Option[Any]): Context
}

object Context {

  case class DefaultContext(id: UUID, persistenceId: PersistenceId, data: Option[Any]) extends Context {
    override def withData(value: Option[Any]): Context = copy(data = value)
  }

  def newContext(id: UUID, persistenceId: PersistenceId, data: Option[Any] = None): Context =
    DefaultContext(id, persistenceId, data)

}
