package com.github.j5ik2o.akka.persistence.dynamodb.serialization

import akka.persistence.SnapshotMetadata

import scala.concurrent.{ ExecutionContext, Future }

trait SnapshotSerializer[T] {

  def serialize(metadata: SnapshotMetadata, snapshot: Any)(implicit ec: ExecutionContext): Future[T]

  def deserialize(t: T)(implicit ec: ExecutionContext): Future[(SnapshotMetadata, Any)]

}
