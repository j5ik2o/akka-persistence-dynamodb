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
package com.github.j5ik2o.akka.persistence.dynamodb.journal.serialization

import akka.persistence.PersistentRepr
import akka.serialization.{ AsyncSerializer, Serialization, Serializer }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

final class ByteArrayJournalSerializer(
    serialization: Serialization,
    separator: String,
    metricsReporter: Option[MetricsReporter],
    traceReporter: Option[TraceReporter]
) extends FlowPersistentReprSerializer[JournalRow] {

  private val serializerAsync: Future[Serializer] = {
    try Future.successful(serialization.serializerFor(classOf[PersistentRepr]))
    catch {
      case NonFatal(ex) =>
        Future.failed(ex)
    }
  }

  private def toBinaryAsync(serializer: Serializer, persistentRepr: PersistentRepr)(implicit
      ec: ExecutionContext
  ): Future[Array[Byte]] = {
    serializer match {
      case async: AsyncSerializer => async.toBinaryAsync(persistentRepr)
      case serializer =>
        try Future.successful(serializer.toBinary(persistentRepr))
        catch {
          case NonFatal(ex) =>
            Future.failed(ex)
        }
    }
  }

  private def fromBinaryAsync(serializer: Serializer, data: Array[Byte])(implicit
      ec: ExecutionContext
  ): Future[PersistentRepr] = {
    val future = serializer match {
      case async: AsyncSerializer => async.fromBinaryAsync(data, classOf[PersistentRepr].getName)
      case serializer =>
        try Future.successful(serializer.fromBinary(data, classOf[PersistentRepr]))
        catch {
          case NonFatal(ex) =>
            Future.failed(ex)
        }
    }
    future.map(_.asInstanceOf[PersistentRepr])
  }

  override def serialize(
      persistentRepr: PersistentRepr,
      tags: Set[String],
      index: Option[Int]
  )(implicit ec: ExecutionContext): Future[JournalRow] = {
    val pid        = PersistenceId(persistentRepr.persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeJournalSerializeJournal(context))

    def future = for {
      serializer <- serializerAsync
      serialized <- toBinaryAsync(serializer, persistentRepr)
    } yield JournalRow(
      PersistenceId(persistentRepr.persistenceId),
      SequenceNumber(persistentRepr.sequenceNr),
      persistentRepr.deleted,
      serialized,
      System.currentTimeMillis(),
      encodeTags(tags, separator)
    )
    val traced = traceReporter.fold(future)(_.traceJournalSerializeJournal(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterJournalSerializeJournal(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorJournalSerializeJournal(newContext, ex))
    }
    traced
  }

  override def deserialize(
      journalRow: JournalRow
  )(implicit ec: ExecutionContext): Future[(PersistentRepr, Set[String], Long)] = {
    val pid        = journalRow.persistenceId
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeJournalDeserializeJournal(context))

    def future = for {
      serializer <- serializerAsync
      deserialized <- fromBinaryAsync(serializer, journalRow.message)
    } yield (deserialized, decodeTags(journalRow.tags, separator), journalRow.ordering)

    val traced = traceReporter.fold(future)(_.traceJournalDeserializeJournal(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterJournalDeserializeJournal(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorJournalDeserializeJournal(newContext, ex))
    }
    traced
  }

}
