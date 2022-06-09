package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.persistence.state.scaladsl.DurableStateUpdateStore

import scala.concurrent.Future

trait DynamoDBDurableStateStore[A] extends DurableStateUpdateStore[A]{
  def getRawObject(persistenceId: String): Future[GetRawObjectResult[A]]
}
