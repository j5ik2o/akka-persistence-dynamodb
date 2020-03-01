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
import akka.serialization.Serialization
import akka.stream.scaladsl.{ Concat, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.DaoSupport
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class ReadJournalDaoImpl(
    asyncClient: DynamoDbAsyncClient,
    serialization: Serialization,
    pluginConfig: QueryPluginConfig,
    override protected val metricsReporter: MetricsReporter
)(implicit ec: ExecutionContext)
    extends ReadJournalDao
    with DaoSupport {

  private val logger = LoggerFactory.getLogger(getClass)

  override protected val streamClient: DynamoDbAkkaClient = DynamoDbAkkaClient(asyncClient)
  protected val shardCount: Int                           = pluginConfig.shardCount
  override val tableName: String                          = pluginConfig.tableName
  override val getJournalRowsIndexName: String            = pluginConfig.getJournalRowsIndexName
  override val columnsDefConfig: JournalColumnsDefConfig  = pluginConfig.columnsDefConfig
  override protected val consistentRead                   = pluginConfig.consistentRead
  override protected val queryBatchSize: Int              = pluginConfig.queryBatchSize

  override def allPersistenceIds(max: Long): Source[PersistenceId, NotUsed] = {
    startTimeSource.flatMapConcat { callStart =>
      logger.debug(s"allPersistenceIdsSource(max = $max): start")
      def loop(
          lastEvaluatedKey: Option[Map[String, AttributeValue]],
          acc: Source[Map[String, AttributeValue], NotUsed],
          count: Long,
          index: Int
      ): Source[Map[String, AttributeValue], NotUsed] = {
        logger.debug(s"index = $index, count = $count")
        val scanRequest = ScanRequest
          .builder()
          .tableName(tableName)
          .select(Select.SPECIFIC_ATTRIBUTES)
          .attributesToGet(columnsDefConfig.deletedColumnName, columnsDefConfig.persistenceIdColumnName)
          .limit(queryBatchSize)
          .exclusiveStartKeyAsScala(lastEvaluatedKey)
          .build()
        Source.single(scanRequest).via(streamClient.scanFlow(1)).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
            val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty && (count + response.count()) < max) {
              logger.debug(s"index = $index, next loop")
              loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            logger.debug(s"allPersistenceIdsSource(max = $max): finished")
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
          }
        }
      }

      loop(None, Source.empty, 0L, 1)
        .filterNot(_(columnsDefConfig.deletedColumnName).bool.get)
        .map(_(columnsDefConfig.persistenceIdColumnName).s.get)
        .fold(Set.empty[String])(_ + _)
        .mapConcat(_.toVector)
        .map(PersistenceId)
        .take(max)
        .map { response =>
          metricsReporter.setAllPersistenceIdsCallDuration(System.nanoTime() - callStart)
          metricsReporter.incrementAllPersistenceIdsCallCounter()
          logger.debug(s"allPersistenceIdsSource(max = $max): finished")
          response
        }.recoverWithRetries(
          attempts = 1, {
            case t: Throwable =>
              metricsReporter.setAllPersistenceIdsCallDuration(System.nanoTime() - callStart)
              metricsReporter.incrementAllPersistenceIdsCallErrorCounter()
              logger.debug(s"allPersistenceIdsSource(max = $max): finished")
              Source.failed(t)
          }
        ).withAttributes(logLevels)
    }
  }

  override def eventsByTag(tag: String, offset: Long, maxOffset: Long, max: Long): Source[JournalRow, NotUsed] = {
    startTimeSource.flatMapConcat { callStart =>
      logger.debug(s"eventsByTag(tag = $tag, offset = $offset, maxOffset = $maxOffset, max = $max): start")
      startTimeSource
        .flatMapConcat { itemStart =>
          def loop(
              lastEvaluatedKey: Option[Map[String, AttributeValue]],
              acc: Source[Map[String, AttributeValue], NotUsed],
              count: Long,
              index: Int
          ): Source[Map[String, AttributeValue], NotUsed] = {
            logger.debug(s"index = $index, count = $count")
            val request = ScanRequest
              .builder()
              .tableName(tableName)
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
              .limit(queryBatchSize)
              .exclusiveStartKeyAsScala(lastEvaluatedKey)
              .build()
            Source
              .single(request).via(streamClient.scanFlow(1)).flatMapConcat { response =>
                metricsReporter.setEventsByTagItemDuration(System.nanoTime() - itemStart)
                if (response.sdkHttpResponse().isSuccessful) {
                  metricsReporter.incrementEventsByTagItemCallCounter()
                  if (response.count() > 0)
                    metricsReporter.addEventsByTagItemCounter(response.count().toLong)
                  val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
                  val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
                  val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                  if (lastEvaluatedKey.nonEmpty) {
                    logger.debug(s"index = $index, next loop")
                    loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
                  } else
                    combinedSource
                } else {
                  metricsReporter.incrementEventsByTagItemCallErrorCounter()
                  val statusCode = response.sdkHttpResponse().statusCode()
                  val statusText = response.sdkHttpResponse().statusText()
                  logger.debug(
                    s"eventsByTag(tag = $tag, offset = $offset, maxOffset = $maxOffset, max = $max): finished"
                  )
                  Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
                }
              }
          }

          loop(None, Source.empty, 0L, 1)
            .map(convertToJournalRow)
            .fold(ArrayBuffer.empty[JournalRow])(_ += _)
            .map(_.sortBy(v => (v.persistenceId.value, v.sequenceNumber.value)))
            .mapConcat(_.toVector)
            .statefulMapConcat { () =>
              val index = new AtomicLong()
              journalRow => List(journalRow.withOrdering(index.incrementAndGet()))
            }
            .filter { row => row.ordering > offset && row.ordering <= maxOffset }
            .take(max)
            .map { response =>
              metricsReporter.setEventsByTagCallDuration(System.nanoTime() - callStart)
              metricsReporter.incrementEventsByTagCallCounter()
              logger.debug(s"eventsByTag(tag = $tag, offset = $offset, maxOffset = $maxOffset, max = $max): finished")
              response
            }.recoverWithRetries(
              attempts = 1, {
                case t: Throwable =>
                  metricsReporter.setEventsByTagCallDuration(System.nanoTime() - callStart)
                  metricsReporter.incrementEventsByTagCallErrorCounter()
                  logger
                    .debug(s"eventsByTag(tag = $tag, offset = $offset, maxOffset = $maxOffset, max = $max): finished")
                  Source.failed(t)
              }
            ).withAttributes(logLevels)
        }
    }
  }

  override def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed] = {
    startTimeSource.flatMapConcat { start =>
      logger.debug(s"journalSequence(offset = $offset, limit = $limit): start")
      startTimeSource.flatMapConcat { requestStart =>
        def loop(
            lastEvaluatedKey: Option[Map[String, AttributeValue]],
            acc: Source[Map[String, AttributeValue], NotUsed],
            count: Long,
            index: Int
        ): Source[Map[String, AttributeValue], NotUsed] = {
          logger.debug(s"index = $index, count = $count")
          val queryRequest = QueryRequest
            .builder().tableName(tableName).select(Select.SPECIFIC_ATTRIBUTES).attributesToGet(
              columnsDefConfig.orderingColumnName
            ).limit(queryBatchSize).exclusiveStartKeyAsScala(lastEvaluatedKey).build()
          Source
            .single(queryRequest)
            .via(streamClient.queryFlow(1)).flatMapConcat { response =>
              metricsReporter.setJournalSequenceItemDuration(System.nanoTime() - requestStart)
              if (response.sdkHttpResponse().isSuccessful) {
                metricsReporter.addJournalSequenceItemCounter(response.count().toLong)
                val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
                val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
                val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                if (lastEvaluatedKey.nonEmpty) {
                  logger.debug(s"index = $index, next loop")
                  loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
                } else
                  combinedSource
              } else {
                metricsReporter.incrementEventsByTagItemCallErrorCounter()
                val statusCode = response.sdkHttpResponse().statusCode()
                val statusText = response.sdkHttpResponse().statusText()
                logger.debug(s"journalSequence(offset = $offset, limit = $limit): finished")
                Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
              }
            }
        }
        loop(None, Source.empty, 0L, 1)
          .map { result => result(columnsDefConfig.orderingColumnName).n.toLong }
          .drop(offset)
          .take(limit)
          .withAttributes(logLevels).map { response =>
            metricsReporter.setJournalSequenceCallDuration(System.nanoTime() - start)
            metricsReporter.incrementJournalSequenceCallCounter()
            logger.debug(s"journalSequence(offset = $offset, limit = $limit): finished")
            response
          }.recoverWithRetries(
            1, {
              case t: Throwable =>
                metricsReporter.setJournalSequenceCallDuration(System.nanoTime() - start)
                metricsReporter.incrementJournalSequenceCallErrorCounter()
                logger.debug(s"journalSequence(offset = $offset, limit = $limit): finished")
                Source.failed(t)
            }
          )

      }
    }
  }

  override def maxJournalSequence(): Source[Long, NotUsed] =
    Source.single(Long.MaxValue)

}
