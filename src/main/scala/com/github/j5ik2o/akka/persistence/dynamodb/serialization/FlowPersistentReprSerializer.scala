package com.github.j5ik2o.akka.persistence.dynamodb.serialization

import akka.NotUsed
import akka.persistence.PersistentRepr
import akka.stream.scaladsl.Flow

trait FlowPersistentReprSerializer[T] extends PersistentReprSerializer[T] {

  def deserializeFlow: Flow[T, (PersistentRepr, Set[String], Long), NotUsed] = {
    Flow[T].map(deserialize).map {
      case Right(r) => r
      case Left(ex) => throw ex
    }
  }

  def deserializeFlowWithoutTags: Flow[T, PersistentRepr, NotUsed] = {
    deserializeFlow.map {
      case (repr, _, _) =>
        repr
    }
  }

}
