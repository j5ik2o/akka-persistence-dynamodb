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
package com.github.j5ik2o.akka.persistence.dynamodb.serialization

import java.util.UUID

import akka.persistence.SnapshotMetadata
import akka.persistence.serialization.Snapshot
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.SnapshotRow

import scala.util.{ Failure, Success }

class ByteArraySnapshotSerializer(serialization: Serialization, metricsReporter: Option[MetricsReporter])
    extends SnapshotSerializer[SnapshotRow] {

  override def serialize(
      metadata: SnapshotMetadata,
      snapshot: Any
  ): Either[Throwable, SnapshotRow] = {
    val pid        = PersistenceId(metadata.persistenceId)
    val context    = MetricsReporter.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreSerializeSnapshot(context))
    serialization
      .serialize(Snapshot(snapshot))
      .map(
        SnapshotRow(PersistenceId(metadata.persistenceId), SequenceNumber(metadata.sequenceNr), metadata.timestamp, _)
      ) match {
      case Success(value) =>
        metricsReporter.foreach(_.afterSnapshotStoreSerializeSnapshot(newContext))
        Right(value)
      case Failure(ex) =>
        metricsReporter.foreach(_.errorSnapshotStoreSerializeSnapshot(newContext, ex))
        Left(ex)
    }
  }

  override def deserialize(snapshotRow: SnapshotRow): Either[Throwable, (SnapshotMetadata, Any)] = {
    val context    = MetricsReporter.newContext(UUID.randomUUID(), snapshotRow.persistenceId)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreDeserializeSnapshot(context))
    serialization
      .deserialize(snapshotRow.snapshot, classOf[Snapshot])
      .map(snapshot => {
        val snapshotMetadata =
          SnapshotMetadata(snapshotRow.persistenceId.asString, snapshotRow.sequenceNumber.value, snapshotRow.created)
        (snapshotMetadata, snapshot.data)
      }) match {
      case Success(value) =>
        metricsReporter.foreach(_.afterSnapshotStoreDeserializeSnapshot(newContext))
        Right(value)
      case Failure(ex) =>
        metricsReporter.foreach(_.errorSnapshotStoreDeserializeSnapshot(newContext, ex))
        Left(ex)
    }
  }

}
