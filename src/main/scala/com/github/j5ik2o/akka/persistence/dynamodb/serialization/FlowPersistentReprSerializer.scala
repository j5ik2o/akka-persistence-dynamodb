package com.github.j5ik2o.akka.persistence.dynamodb.serialization

import akka.NotUsed
import akka.persistence.PersistentRepr
import akka.stream.scaladsl.Flow

trait FlowPersistentReprSerializer[T] extends PersistentReprSerializer[T] {

  def deserializeFlow: Flow[T, Either[Throwable, (PersistentRepr, Set[String], Long)], NotUsed] = {
    Flow[T].map(deserialize)
  }

  def deserializeFlowWithoutTags: Flow[T, Either[Throwable, PersistentRepr], NotUsed] = {
    def keepPersistentRepr(tup: (PersistentRepr, Set[String], Long)): PersistentRepr = tup match {
      case (repr, _, _) => repr
    }
    deserializeFlow.map(_.map(keepPersistentRepr))
  }

}
