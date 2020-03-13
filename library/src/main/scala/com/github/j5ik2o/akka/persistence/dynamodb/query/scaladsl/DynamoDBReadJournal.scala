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
package com.github.j5ik2o.akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.pattern._
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.persistence.{ Persistence, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.Attributes
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalSequenceRetrievalConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.query.JournalSequenceActor
import com.github.j5ik2o.akka.persistence.dynamodb.query.JournalSequenceActor.{ GetMaxOrderingId, MaxOrderingId }
import com.github.j5ik2o.akka.persistence.dynamodb.query.dao.ReadJournalDaoImpl
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDbClientBuilderUtils, HttpClientBuilderUtils }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.collection.immutable.Iterable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

object DynamoDBReadJournal {
  final val Identifier = "j5ik2o.dynamo-db-read-journal"

  private sealed trait FlowControl

  /** Keep querying - used when we are sure that there is more events to fetch */
  private case object Continue extends FlowControl

  /**
    * Keep querying with delay - used when we have consumed all events,
    * but want to poll for future events
    */
  private case object ContinueDelayed extends FlowControl

  /** Stop querying - used when we reach the desired offset  */
  private case object Stop extends FlowControl
}

class DynamoDBReadJournal(config: Config, configPath: String)(implicit system: ExtendedActorSystem)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery {
  private val logger                = LoggerFactory.getLogger(getClass)
  implicit val ec: ExecutionContext = system.dispatcher

  protected val pluginConfig: QueryPluginConfig =
    QueryPluginConfig.fromConfig(config)

  import pluginConfig._

  protected val journalSequenceRetrievalConfig: JournalSequenceRetrievalConfig =
    pluginConfig.journalSequenceRetrievalConfig

  private val asyncHttpClientBuilder = HttpClientBuilderUtils.setup(pluginConfig.clientConfig)

  private val dynamoDbAsyncClientBuilder =
    DynamoDbClientBuilderUtils.setup(pluginConfig.clientConfig, asyncHttpClientBuilder.build())

  protected val javaAsyncClient: JavaDynamoDbAsyncClient = dynamoDbAsyncClientBuilder.build()

  protected val asyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(javaAsyncClient)
  protected val streamClient: DynamoDbAkkaClient = DynamoDbAkkaClient(asyncClient)
  private val serialization: Serialization       = SerializationExtension(system)

  val metricsReporter: MetricsReporter = MetricsReporter.create(pluginConfig.metricsReporterClassName)

  private val readJournalDao = new ReadJournalDaoImpl(asyncClient, serialization, pluginConfig, metricsReporter)

  private val writePluginId = config.getString("write-plugin")
  private val eventAdapters = Persistence(system).adaptersFor(writePluginId)

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, pluginConfig.tagSeparator)

  private val delaySource =
    Source.tick(pluginConfig.refreshInterval, 0.seconds, 0).take(1)

  private val logLevels = Attributes.logLevels(
    onElement = Attributes.LogLevels.Debug,
    onFailure = Attributes.LogLevels.Error,
    onFinish = Attributes.LogLevels.Debug
  )

  private val refreshInterval = pluginConfig.refreshInterval

  private lazy val journalSequenceActor = system.systemActorOf(
    JournalSequenceActor.props(readJournalDao, journalSequenceRetrievalConfig),
    s"$configPath.akka-persistence-dynamodb-journal-sequence-actor"
  )

  override def currentPersistenceIds(): Source[String, NotUsed] =
    readJournalDao.allPersistenceIds(Long.MaxValue).map(_.value)

  override def persistenceIds(): Source[String, NotUsed] =
    Source
      .repeat(0).flatMapConcat(_ =>
        Source.tick(refreshInterval, 0.seconds, 0).take(1).flatMapConcat(_ => currentPersistenceIds())
      )
      .statefulMapConcat[String] { () =>
        var knownIds = Set.empty[String]
        def next(id: String): Iterable[String] = {
          val xs = Set(id).diff(knownIds)
          knownIds += id
          xs
        }
        id =>
          logger.debug("knownIds.size = {}", knownIds.size)
          next(id)
      }.log("persistenceIds")
      .withAttributes(logLevels)

  private def adaptEvents(persistentRepr: PersistentRepr): Vector[PersistentRepr] = {
    val adapter = eventAdapters.get(persistentRepr.payload.getClass)
    adapter.fromJournal(persistentRepr.payload, persistentRepr.manifest).events.map(persistentRepr.withPayload).toVector
  }

  private def currentJournalEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[PersistentRepr, NotUsed] =
    readJournalDao
      .getMessages(
        PersistenceId(persistenceId),
        SequenceNumber(fromSequenceNr),
        SequenceNumber(toSequenceNr),
        Long.MaxValue
      )
      .via(serializer.deserializeFlowWithoutTags)
      .log("currentJournalEventsByPersistenceId")
      .withAttributes(logLevels)

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] =
    currentJournalEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
      .mapConcat(adaptEvents)
      .map(repr => new EventEnvelope(Sequence(repr.sequenceNr), repr.persistenceId, repr.sequenceNr, repr.payload, 0L))
      .log("currentEventsByPersistenceId")
      .withAttributes(logLevels)

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] =
    Source
      .unfoldAsync[Long, Seq[EventEnvelope]](Math.max(1, fromSequenceNr)) { from: Long =>
        def nextFromSeqNr(xs: Seq[EventEnvelope]): Long = {
          if (xs.isEmpty) from else xs.map(_.sequenceNr).max + 1
        }
        from match {
          case x if x > toSequenceNr => Future.successful(None)
          case _ =>
            delaySource
              .flatMapConcat { _ =>
                currentJournalEventsByPersistenceId(persistenceId, from, toSequenceNr)
                  .take(queryBatchSize)
              }
              .mapConcat(adaptEvents)
              .map(repr =>
                new EventEnvelope(Sequence(repr.sequenceNr), repr.persistenceId, repr.sequenceNr, repr.payload, 0L)
              )
              .runWith(Sink.seq).map { xs =>
                val newFromSeqNr = nextFromSeqNr(xs)
                Some((newFromSeqNr, xs))
              }
        }
      }.mapConcat(_.toVector)

  private def currentJournalEventsByTag(
      tag: String,
      offset: Long,
      max: Long,
      latestOrdering: MaxOrderingId
  ): Source[EventEnvelope, NotUsed] = {
    if (latestOrdering.maxOrdering < offset) Source.empty
    else {
      readJournalDao
        .eventsByTag(tag, offset, latestOrdering.maxOrdering, max)
        .via(serializer.deserializeFlow)
        .mapConcat {
          case (repr, _, ordering) =>
            adaptEvents(repr).map(r =>
              new EventEnvelope(Sequence(ordering), r.persistenceId, r.sequenceNr, r.payload, 0L)
            )
        }
    }
  }

  private def eventsByTag(
      tag: String,
      offset: Long,
      terminateAfterOffset: Option[Long]
  ): Source[EventEnvelope, NotUsed] = {
    import DynamoDBReadJournal._
    implicit val askTimeout: Timeout = Timeout(journalSequenceRetrievalConfig.askTimeout)
    Source
      .unfoldAsync[(Long, FlowControl), Seq[EventEnvelope]]((offset, Continue)) {
        case (from, control) =>
          def retrieveNextBatch() = {
            for {
              queryUntil <- journalSequenceActor.ask(GetMaxOrderingId).mapTo[MaxOrderingId]
              xs         <- currentJournalEventsByTag(tag, from, queryBatchSize, queryUntil).runWith(Sink.seq)
            } yield {

              val hasMoreEvents = xs.size == queryBatchSize
              val control =
                terminateAfterOffset match {
                  // we may stop if target is behind queryUntil and we don't have more events to fetch
                  case Some(target) if !hasMoreEvents && target <= queryUntil.maxOrdering => Stop
                  // We may also stop if we have found an event with an offset >= target
                  case Some(target) if xs.exists {
                        _.offset match {
                          case Sequence(value)  => value >= target
                          case TimeBasedUUID(_) => true
                          case NoOffset         => true
                        }
                      } =>
                    Stop

                  // otherwise, disregarding if Some or None, we must decide how to continue
                  case _ =>
                    if (hasMoreEvents) Continue else ContinueDelayed
                }

              val nextStartingOffset = if (xs.isEmpty) {
                /* If no events matched the tag between `from` and `maxOrdering` then there is no need to execute the exact
                 * same query again. We can continue querying from `maxOrdering`, which will save some load on the db.
                 * (Note: we may never return a value smaller than `from`, otherwise we might return duplicate events) */
                math.max(from, queryUntil.maxOrdering)
              } else {
                // Continue querying from the largest offset
                xs.map { ee =>
                  val Sequence(v) = ee.offset
                  v
                }.max
              }
              Some((nextStartingOffset, control), xs)
            }
          }

          control match {
            case Stop     => Future.successful(None)
            case Continue => retrieveNextBatch()
            case ContinueDelayed =>
              akka.pattern.after(refreshInterval, system.scheduler)(retrieveNextBatch())
          }

      }.mapConcat(_.toVector)
  }

  def currentEventsByTag(tag: String, offset: Long): Source[EventEnvelope, NotUsed] =
    readJournalDao
      .maxJournalSequence()
      .flatMapConcat { maxOrderingInDb => eventsByTag(tag, offset, terminateAfterOffset = Some(maxOrderingInDb)) }

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    offset match {
      case NoOffset =>
        currentEventsByTag(tag, 0)
      case Sequence(value) =>
        currentEventsByTag(tag, value)
    }
  }

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    offset match {
      case NoOffset =>
        eventsByTag(tag, 0, terminateAfterOffset = None)
      case Sequence(value) =>
        eventsByTag(tag, value, terminateAfterOffset = None)
    }
  }
}
