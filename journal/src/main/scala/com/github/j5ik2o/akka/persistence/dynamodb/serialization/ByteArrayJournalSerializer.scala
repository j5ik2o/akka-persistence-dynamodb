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

import akka.persistence.PersistentRepr
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

import scala.util.{ Failure, Success }

class ByteArrayJournalSerializer(
    serialization: Serialization,
    separator: String,
    metricsReporter: Option[MetricsReporter]
) extends FlowPersistentReprSerializer[JournalRow] {

  override def serialize(
      persistentRepr: PersistentRepr,
      tags: Set[String],
      index: Option[Int]
  ): Either[Throwable, JournalRow] = {
    val pid        = PersistenceId(persistentRepr.persistenceId)
    val context    = MetricsReporter.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeJournalSerializeJournal(context))
    serialization
      .serialize(persistentRepr)
      .map(
        JournalRow(
          PersistenceId(persistentRepr.persistenceId),
          SequenceNumber(persistentRepr.sequenceNr),
          persistentRepr.deleted,
          _,
          System.currentTimeMillis(),
          encodeTags(tags, separator)
        )
      ) match {
      case Success(value) =>
        metricsReporter.foreach(_.afterJournalSerializeJournal(newContext))
        Right(value)
      case Failure(ex) =>
        metricsReporter.foreach(_.errorJournalSerializeJournal(newContext, ex))
        Left(ex)
    }
  }

  override def deserialize(journalRow: JournalRow): Either[Throwable, (PersistentRepr, Set[String], Long)] = {
    val pid        = journalRow.persistenceId
    val context    = MetricsReporter.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeJournalDeserializeJournal(context))
    serialization
      .deserialize(journalRow.message, classOf[PersistentRepr])
      .map((_, decodeTags(journalRow.tags, separator), journalRow.ordering)) match {
      case Success(value) =>
        metricsReporter.foreach(_.afterJournalDeserializeJournal(newContext))
        Right(value)
      case Failure(ex) =>
        metricsReporter.foreach(_.errorJournalDeserializeJournal(newContext, ex))
        Left(ex)
    }
  }
}
