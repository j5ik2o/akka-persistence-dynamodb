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

import akka.persistence.SnapshotMetadata
import akka.persistence.serialization.Snapshot
import akka.serialization.Serialization

import scala.util.{ Failure, Success }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.PersistenceId
import com.github.j5ik2o.akka.persistence.dynamodb.journal.SequenceNumber

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
      .map(
        SnapshotRow(PersistenceId(metadata.persistenceId), SequenceNumber(metadata.sequenceNr), metadata.timestamp, _)
      ) match {
      case Success(value) => Right(value)
      case Failure(ex)    => Left(ex)
    }
  }

  override def deserialize(snapshotRow: SnapshotRow): Either[Throwable, (SnapshotMetadata, Any)] = {
    serialization
      .deserialize(snapshotRow.snapshot, classOf[Snapshot])
      .map(snapshot => {
        val snapshotMetadata =
          SnapshotMetadata(snapshotRow.persistenceId.value, snapshotRow.sequenceNumber.value, snapshotRow.created)
        (snapshotMetadata, snapshot.data)
      }) match {
      case Success(value) => Right(value)
      case Failure(ex)    => Left(ex)
    }
  }
}
