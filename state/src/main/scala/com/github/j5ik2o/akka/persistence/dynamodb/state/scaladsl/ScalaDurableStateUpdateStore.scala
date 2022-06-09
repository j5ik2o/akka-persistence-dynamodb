package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.persistence.state.scaladsl.DurableStateUpdateStore
import com.github.j5ik2o.akka.persistence.dynamodb.state.GetRawObjectResult

import scala.concurrent.Future

trait ScalaDurableStateUpdateStore[A] extends DurableStateUpdateStore[A] {
  def getRawObject(persistenceId: String): Future[GetRawObjectResult[A]]
}
