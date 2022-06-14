/*
 * Copyright 2017 Dennis Vriend
 * Copyright 2019 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao

import akka.NotUsed
import akka.persistence.SnapshotMetadata
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

import scala.concurrent.ExecutionContext

trait SnapshotDao {

  def dispose(): Unit

  def deleteAllSnapshots(persistenceId: PersistenceId)(implicit ec: ExecutionContext): Source[Unit, NotUsed]

  def deleteUpToMaxSequenceNr(persistenceId: PersistenceId, maxSequenceNr: SequenceNumber)(implicit
      ec: ExecutionContext
  ): Source[Unit, NotUsed]

  def deleteUpToMaxTimestamp(persistenceId: PersistenceId, maxTimestamp: Long)(implicit
      ec: ExecutionContext
  ): Source[Unit, NotUsed]

  def deleteUpToMaxSequenceNrAndMaxTimestamp(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber,
      maxTimestamp: Long
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed]

  def latestSnapshot(persistenceId: PersistenceId)(implicit
      ec: ExecutionContext
  ): Source[Option[(SnapshotMetadata, Any)], NotUsed]

  def snapshotForMaxTimestamp(
      persistenceId: PersistenceId,
      timestamp: Long
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed]

  def snapshotForMaxSequenceNr(
      persistenceId: PersistenceId,
      sequenceNr: SequenceNumber
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed]

  def snapshotForMaxSequenceNrAndMaxTimestamp(
      persistenceId: PersistenceId,
      sequenceNr: SequenceNumber,
      timestamp: Long
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed]

  def delete(persistenceId: PersistenceId, sequenceNr: SequenceNumber): Source[Unit, NotUsed]

  def save(snapshotMetadata: SnapshotMetadata, snapshot: Any)(implicit ec: ExecutionContext): Source[Unit, NotUsed]

}
