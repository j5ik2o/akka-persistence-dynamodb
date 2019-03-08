package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao

import akka.NotUsed
import akka.persistence.SnapshotMetadata
import akka.stream.scaladsl.Source

trait SnapshotDao {

  def deleteAllSnapshots(persistenceId: String): Source[Unit, NotUsed]

  def deleteUpToMaxSequenceNr(persistenceId: String, maxSequenceNr: Long): Source[Unit, NotUsed]

  def deleteUpToMaxTimestamp(persistenceId: String, maxTimestamp: Long): Source[Unit, NotUsed]

  def deleteUpToMaxSequenceNrAndMaxTimestamp(persistenceId: String,
                                             maxSequenceNr: Long,
                                             maxTimestamp: Long): Source[Unit, NotUsed]

  def latestSnapshot(persistenceId: String): Source[Option[(SnapshotMetadata, Any)], NotUsed]

  def snapshotForMaxTimestamp(persistenceId: String, timestamp: Long): Source[Option[(SnapshotMetadata, Any)], NotUsed]

  def snapshotForMaxSequenceNr(persistenceId: String,
                               sequenceNr: Long): Source[Option[(SnapshotMetadata, Any)], NotUsed]

  def snapshotForMaxSequenceNrAndMaxTimestamp(persistenceId: String,
                                              sequenceNr: Long,
                                              timestamp: Long): Source[Option[(SnapshotMetadata, Any)], NotUsed]

  def delete(persistenceId: String, sequenceNr: Long): Source[Unit, NotUsed]

  def save(snapshotMetadata: SnapshotMetadata, snapshot: Any): Source[Unit, NotUsed]

}
