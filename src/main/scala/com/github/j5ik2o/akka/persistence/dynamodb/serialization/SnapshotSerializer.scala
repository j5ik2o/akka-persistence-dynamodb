package com.github.j5ik2o.akka.persistence.dynamodb.serialization
import akka.persistence.SnapshotMetadata

trait SnapshotSerializer[T] {

  def serialize(metadata: SnapshotMetadata, snapshot: Any): Either[Throwable, T]

  def deserialize(t: T): Either[Throwable, (SnapshotMetadata, Any)]

}
