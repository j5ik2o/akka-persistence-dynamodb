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

import akka.persistence.SnapshotMetadata
import akka.persistence.serialization.Snapshot
import akka.serialization.{ AsyncSerializer, Serialization, Serializer }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.SnapshotRow
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

final class ByteArraySnapshotSerializer(
    serialization: Serialization,
    metricsReporter: Option[MetricsReporter],
    traceReporter: Option[TraceReporter]
) extends SnapshotSerializer[SnapshotRow] {

  private def serializerAsync: Future[Serializer] = {
    try Future.successful(serialization.serializerFor(classOf[Snapshot]))
    catch {
      case ex: Throwable =>
        Future.failed(ex)
    }
  }

  private def toBinaryAsync(serializer: Serializer, snapshot: Snapshot): Future[Array[Byte]] = {
    serializer match {
      case async: AsyncSerializer => async.toBinaryAsync(snapshot)
      case serializer =>
        try Future.successful(serializer.toBinary(snapshot))
        catch {
          case ex: Throwable =>
            Future.failed(ex)
        }
    }
  }

  private def fromBinaryAsync(serializer: Serializer, data: Array[Byte])(implicit
      ec: ExecutionContext
  ): Future[Snapshot] = {
    val future = serializer match {
      case async: AsyncSerializer => async.fromBinaryAsync(data, classOf[Snapshot].getName)
      case serializer =>
        try Future.successful(serializer.fromBinary(data, classOf[Snapshot]))
        catch {
          case ex: Throwable =>
            Future.failed(ex)
        }
    }
    future.map(_.asInstanceOf[Snapshot])
  }

  override def serialize(
      metadata: SnapshotMetadata,
      snapshot: Any
  )(implicit ec: ExecutionContext): Future[SnapshotRow] = {
    val pid        = PersistenceId(metadata.persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreSerializeSnapshot(context))

    def future = for {
      serializer <- serializerAsync
      serialized <- toBinaryAsync(serializer, Snapshot(snapshot))
    } yield SnapshotRow(
      PersistenceId(metadata.persistenceId),
      SequenceNumber(metadata.sequenceNr),
      metadata.timestamp,
      serialized
    )

    val traced = traceReporter.fold(future)(_.traceSnapshotStoreSerializeSnapshot(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterSnapshotStoreSerializeSnapshot(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorSnapshotStoreSerializeSnapshot(newContext, ex))
    }

    traced
  }

  override def deserialize(snapshotRow: SnapshotRow)(implicit ec: ExecutionContext): Future[(SnapshotMetadata, Any)] = {
    val context    = Context.newContext(UUID.randomUUID(), snapshotRow.persistenceId)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreDeserializeSnapshot(context))

    def future = for {
      serializer <- serializerAsync
      deserialized <- fromBinaryAsync(serializer, snapshotRow.snapshot)
    } yield {
      val snapshotMetadata =
        SnapshotMetadata(snapshotRow.persistenceId.asString, snapshotRow.sequenceNumber.value, snapshotRow.created)
      (snapshotMetadata, deserialized.data)
    }
    val traced = traceReporter.fold(future)(_.traceSnapshotStoreDeserializeSnapshot(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterSnapshotStoreDeserializeSnapshot(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorSnapshotStoreDeserializeSnapshot(newContext, ex))
    }
    traced
  }

}
