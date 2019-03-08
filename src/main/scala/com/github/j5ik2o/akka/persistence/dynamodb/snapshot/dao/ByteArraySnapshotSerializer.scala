package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao

import akka.persistence.SnapshotMetadata
import akka.persistence.serialization.Snapshot
import akka.serialization.Serialization

trait SnapshotSerializer[T] {
  def serialize(metadata: SnapshotMetadata, snapshot: Any): Either[Throwable, T]

  def deserialize(t: T): Either[Throwable, (SnapshotMetadata, Any)]
}

class ByteArraySnapshotSerializer(serialization: Serialization) extends SnapshotSerializer[SnapshotRow] {
  override def serialize(
      metadata: SnapshotMetadata,
      snapshot: Any
  ): Either[Throwable, SnapshotRow] = {
    serialization
      .serialize(Snapshot(snapshot))
      .map(SnapshotRow(metadata.persistenceId, metadata.sequenceNr, metadata.timestamp, _)).toEither
  }

  override def deserialize(snapshotRow: SnapshotRow): Either[Throwable, (SnapshotMetadata, Any)] = {
    serialization
      .deserialize(snapshotRow.snapshot, classOf[Snapshot])
      .map(snapshot => {
        val snapshotMetadata =
          SnapshotMetadata(snapshotRow.persistenceId, snapshotRow.sequenceNumber, snapshotRow.created)
        (snapshotMetadata, snapshot.data)
      }).toEither
  }
}
