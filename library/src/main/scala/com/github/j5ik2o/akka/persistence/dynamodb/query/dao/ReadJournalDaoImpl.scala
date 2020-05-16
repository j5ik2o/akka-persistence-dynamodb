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

import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.stream.scaladsl.{ Concat, Flow, Source, SourceUtils }
import akka.stream.{ ActorMaterializer, Attributes }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ DaoSupport, V2JournalRowReadDriver }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.immutable.Set
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.Try

class ReadJournalDaoImpl(
    asyncClient: DynamoDbAsyncClient,
    pluginConfig: QueryPluginConfig,
    override val serializer: FlowPersistentReprSerializer[JournalRow],
    override protected val metricsReporter: MetricsReporter
)(implicit val ec: ExecutionContext, system: ActorSystem)
    extends ReadJournalDao
    with DaoSupport {

  private val startTimeSource: Source[Long, NotUsed] =
    SourceUtils
      .lazySource(() => Source.single(System.nanoTime())).mapMaterializedValue(_ => NotUsed)

  private val logLevels: Attributes = Attributes.logLevels(
    onElement = Attributes.LogLevels.Debug,
    onFailure = Attributes.LogLevels.Error,
    onFinish = Attributes.LogLevels.Debug
  )

  implicit val mat = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(getClass)

  override val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig
  private val streamClient                               = DynamoDbAkkaClient(asyncClient)

  override protected def journalRowDriver: V2JournalRowReadDriver = new V2JournalRowReadDriver(
    Some(asyncClient),
    None,
    pluginConfig,
    metricsReporter
  )

  override def allPersistenceIds(max: Long): Source[PersistenceId, NotUsed] = {
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = {
      val scanRequest = ScanRequest
        .builder()
        .tableName(pluginConfig.tableName)
        .select(Select.SPECIFIC_ATTRIBUTES)
        .attributesToGet(columnsDefConfig.deletedColumnName, columnsDefConfig.persistenceIdColumnName)
        .limit(pluginConfig.scanBatchSize)
        .exclusiveStartKeyAsScala(lastEvaluatedKey)
        .consistentRead(pluginConfig.consistentRead)
        .build()
      Source.single(scanRequest).via(streamClient.scanFlow(1)).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
          val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
          val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
          if (lastEvaluatedKey.nonEmpty && (count + response.count()) < max) {
            // logger.debug(s"index = $index, next loop")
            loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
          } else
            combinedSource
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
        }
      }
    }

    loop(None, Source.empty, 0L, 1)
      .filterNot(_(columnsDefConfig.deletedColumnName).bool.get)
      .map(_(columnsDefConfig.persistenceIdColumnName).s.get)
      .fold(Set.empty[String])(_ + _)
      .mapConcat(_.toVector)
      .map(PersistenceId.apply)
      .take(max)
      .withAttributes(logLevels)
  }

  private def perfectlyMatchTag(tag: String, separator: String): Flow[JournalRow, JournalRow, NotUsed] =
    Flow[JournalRow].filter(_.tags.exists(tags => tags.split(separator).contains(tag)))

  override def eventsByTag(
      tag: String,
      offset: Long,
      maxOffset: Long,
      max: Long
  ): Source[Try[(PersistentRepr, Set[String], Long)], NotUsed] = {
    eventsByTagAsJournalRow(tag, offset, maxOffset, max)
      .via(perfectlyMatchTag(tag, pluginConfig.tagSeparator))
      .via(serializer.deserializeFlowAsTry)
  }

  override def eventsByTagAsJournalRow(
      tag: String,
      offset: Long,
      maxOffset: Long,
      max: Long
  ): Source[JournalRow, NotUsed] = {
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] =
      startTimeSource
        .flatMapConcat { itemStart =>
          // logger.debug(s"index = $index, count = $count")
          val scanRequest = ScanRequest
            .builder()
            .tableName(pluginConfig.tableName)
            .indexName(pluginConfig.tagsIndexName)
            .filterExpression("contains(#tags, :tag)")
            .expressionAttributeNamesAsScala(
              Map("#tags" -> columnsDefConfig.tagsColumnName)
            )
            .expressionAttributeValuesAsScala(
              Map(
                ":tag" -> AttributeValue.builder().s(tag).build()
              )
            )
            .limit(pluginConfig.scanBatchSize)
            .exclusiveStartKeyAsScala(lastEvaluatedKey)
            .build()
          Source
            .single(scanRequest).via(streamClient.scanFlow(1)).flatMapConcat { response =>
              metricsReporter.setEventsByTagItemDuration(System.nanoTime() - itemStart)
              if (response.sdkHttpResponse().isSuccessful) {
                metricsReporter.incrementEventsByTagItemCallCounter()
                if (response.count() > 0)
                  metricsReporter.addEventsByTagItemCounter(response.count().toLong)
                val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
                val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
                val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                if (lastEvaluatedKey.nonEmpty) {
                  // logger.debug(s"index = $index, next loop")
                  loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
                } else
                  combinedSource
              } else {
                metricsReporter.incrementEventsByTagItemCallErrorCounter()
                val statusCode = response.sdkHttpResponse().statusCode()
                val statusText = response.sdkHttpResponse().statusText()
                Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
              }
            }
        }

    loop(None, Source.empty, 0L, 1)
      .map(convertToJournalRow)
      .fold(ArrayBuffer.empty[JournalRow])(_ += _)
      .map(_.sortBy(journalRow => (journalRow.persistenceId.asString, journalRow.sequenceNumber.value)))
      .mapConcat(_.toVector)
      .statefulMapConcat { () =>
        val index = new AtomicLong()
        journalRow => List(journalRow.withOrdering(index.incrementAndGet()))
      }
      .filter(journalRow => journalRow.ordering > offset && journalRow.ordering <= maxOffset)
      .take(max)
      .withAttributes(logLevels)

  }

  override def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed] = {
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = startTimeSource.flatMapConcat { requestStart =>
      val scanRequest = ScanRequest
        .builder().tableName(pluginConfig.tableName).select(Select.SPECIFIC_ATTRIBUTES).attributesToGet(
          columnsDefConfig.orderingColumnName
        ).limit(pluginConfig.scanBatchSize).exclusiveStartKeyAsScala(lastEvaluatedKey)
        .consistentRead(pluginConfig.consistentRead)
        .build()
      Source
        .single(scanRequest)
        .via(streamClient.scanFlow(1)).flatMapConcat { response =>
          metricsReporter.setJournalSequenceItemDuration(System.nanoTime() - requestStart)
          if (response.sdkHttpResponse().isSuccessful) {
            metricsReporter.addJournalSequenceItemCounter(response.count().toLong)
            val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
            val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty) {
              // logger.debug(s"index = $index, next loop")
              loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
            } else
              combinedSource
          } else {
            metricsReporter.incrementEventsByTagItemCallErrorCounter()
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
          }
        }
    }
    loop(None, Source.empty, 0L, 1)
      .map { result => result(columnsDefConfig.orderingColumnName).n.toLong }
      .drop(offset)
      .take(limit)

  }

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
