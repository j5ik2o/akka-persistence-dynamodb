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
package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import akka.NotUsed
import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.config.QueryPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ DaoSupport, JournalRowReadDriver }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer

import scala.collection.immutable.Set
import scala.concurrent.ExecutionContext
import scala.util.Try
import akka.stream.Materializer

class ReadJournalDaoImpl(
    queryProcessor: QueryProcessor,
    override protected val journalRowDriver: JournalRowReadDriver,
    pluginConfig: QueryPluginConfig,
    override val serializer: FlowPersistentReprSerializer[JournalRow],
    override protected val metricsReporter: Option[MetricsReporter]
)(implicit val ec: ExecutionContext, system: ActorSystem)
    extends ReadJournalDao
    with DaoSupport {

  implicit val mat: Materializer = ActorMaterializer()

  override def allPersistenceIds(max: Long): Source[PersistenceId, NotUsed] = queryProcessor.allPersistenceIds(max)

  private def perfectlyMatchTag(tag: String, separator: String): Flow[JournalRow, JournalRow, NotUsed] =
    Flow[JournalRow].filter(_.tags.exists(tags => tags.split(separator).contains(tag)))

  override def eventsByTag(
      tag: String,
      offset: Long,
      maxOffset: Long,
      max: Long
  ): Source[Try[(PersistentRepr, Set[String], Long)], NotUsed] =
    eventsByTagAsJournalRow(tag, offset, maxOffset, max)
      .via(perfectlyMatchTag(tag, pluginConfig.tagSeparator))
      .via(serializer.deserializeFlowAsTry)

  override def eventsByTagAsJournalRow(
      tag: String,
      offset: Long,
      maxOffset: Long,
      max: Long
  ): Source[JournalRow, NotUsed] = queryProcessor.eventsByTagAsJournalRow(tag, offset, maxOffset, max)

  override def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed] =
    queryProcessor.journalSequence(offset, limit)

  override def getMessagesAsJournalRow(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean]
  ): Source[JournalRow, NotUsed] =
    journalRowDriver.getJournalRows(persistenceId, fromSequenceNr, toSequenceNr, max, deleted)

  override def maxJournalSequence(): Source[Long, NotUsed] = {
    Source.single(Long.MaxValue)
  }

}
