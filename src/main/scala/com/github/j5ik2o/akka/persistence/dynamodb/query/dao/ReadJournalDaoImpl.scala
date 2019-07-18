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

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.serialization.Serialization
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.jmx.MetricsFunctions
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.DaoSupport
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future }

class ReadJournalDaoImpl(
    asyncClient: DynamoDbAsyncClient,
    serialization: Serialization,
    pluginConfig: QueryPluginConfig,
    override protected val metricsFunctions: MetricsFunctions
)(implicit ec: ExecutionContext)
    extends ReadJournalDao
    with DaoSupport {

  import pluginConfig._

  private val logger = LoggerFactory.getLogger(getClass)

  override protected val streamClient: DynamoDbAkkaClient = DynamoDbAkkaClient(asyncClient)

  override val tableName: String                         = pluginConfig.tableName
  override val getJournalRowsIndexName: String           = pluginConfig.getJournalRowsIndexName
  override val parallelism: Int                          = pluginConfig.parallelism
  override val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig

  override def allPersistenceIdsSource(max: Long): Source[PersistenceId, NotUsed] = {
    logger.debug("allPersistenceIdsSource: max = {}", max)
    type State = Option[Map[String, AttributeValue]]
    type Elm   = Seq[Map[String, AttributeValue]]
    Source
      .single(System.nanoTime()).flatMapConcat { start =>
        Source
          .unfoldAsync[Option[State], Elm](None) {
            case None =>
              scan(None)
                .map { v =>
                  if (v.lastEvaluatedKeyAsScala.isEmpty) Some(None, v.itemsAsScala.get)
                  else Some(Some(v.lastEvaluatedKeyAsScala), v.itemsAsScala.get)
                }
            case Some(Some(lastKey)) if lastKey.nonEmpty =>
              scan(Some(lastKey))
                .map { v =>
                  if (v.lastEvaluatedKeyAsScala.isEmpty) Some(None, v.itemsAsScala.get)
                  else Some(Some(v.lastEvaluatedKeyAsScala), v.itemsAsScala.get)
                }
            case _ =>
              Future.successful(None)
          }.log("unfold")
          .takeWhile(_.nonEmpty)
          .mapConcat(_.toVector)
          .filterNot { v =>
            v(columnsDefConfig.deletedColumnName).bool.get
          }
          .map { map =>
            map(columnsDefConfig.persistenceIdColumnName).s.get
          }
          .fold(Set.empty[String]) { case (r, e) => r + e }
          .mapConcat(_.toVector)
          .map(PersistenceId)
          .take(max)
          .withAttributes(logLevels).map { response =>
            metricsFunctions.setAllPersistenceIdsTotalDuration(System.nanoTime() - start)
            metricsFunctions.incrementAllPersistenceIdsTotalCounter()
            response
          }
      }
  }

  private def scan(lastKey: Option[Map[String, AttributeValue]]): Future[ScanResponse] = {
    Future.successful(System.nanoTime()).flatMap { start =>
      asyncClient
        .scan(
          ScanRequest
            .builder()
            .tableName(tableName)
            .select(Select.ALL_ATTRIBUTES)
            .limit(batchSize)
            .exclusiveStartKeyAsScala(lastKey).build()
        ).map { response =>
          metricsFunctions.setAllPersistenceIdsDuration(System.nanoTime() - start)
          metricsFunctions.addAllPersistenceIdsCounter(response.count().toLong)
          response
        }
    }
  }

  override def eventsByTag(tag: String, offset: Long, maxOffset: Long, max: Long): Source[JournalRow, NotUsed] = {
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
      ).build()
    Source.single(System.nanoTime()).flatMapConcat { start =>
      Source
        .single(System.nanoTime()).flatMapConcat { requestStart =>
          Source
            .single(request).via(streamClient.scanFlow(parallelism)).map { response =>
              metricsFunctions.setEventsByTagDuration(System.nanoTime() - requestStart)
              metricsFunctions.addEventsByTagCounter(response.count().toLong)
              response
            }
        }
        .map { response =>
          response.itemsAsScala.getOrElse(Seq.empty)
        }
        .takeWhile(_.nonEmpty)
        .mapConcat(_.toVector)
        .map(convertToJournalRow)
        .fold(ArrayBuffer.empty[JournalRow]) { case (r, e) => r append e; r }
        .map(_.sortBy(v => (v.persistenceId.value, v.sequenceNumber.value)))
        .mapConcat(_.toVector)
        .statefulMapConcat { () =>
          val index = new AtomicLong()
          journalRow => List(journalRow.withOrdering(index.incrementAndGet()))
        }
        .filter { row =>
          row.ordering > offset && row.ordering <= maxOffset
        }
        .take(max)
        .withAttributes(logLevels).map { response =>
          metricsFunctions.setEventsByTagTotalDuration(System.nanoTime() - start)
          metricsFunctions.incrementEventsByTagTotalCounter()
          response
        }
    }
  }

//  private def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
//    import com.github.j5ik2o.akka.persistence.dynamodb.journal.SequenceNumber
//    JournalRow(
//      persistenceId = PersistenceId(map(columnsDefConfig.persistenceIdColumnName).string.get),
//      sequenceNumber = SequenceNumber(map(columnsDefConfig.sequenceNrColumnName).number.get.toLong),
//      deleted = map(columnsDefConfig.deletedColumnName).bool.get,
//      message = map.get(columnsDefConfig.messageColumnName).flatMap(_.binary).get,
//      ordering = map(columnsDefConfig.orderingColumnName).number.get.toLong,
//      tags = map.get(columnsDefConfig.tagsColumnName).flatMap(_.string)
//    )
//  }

  override def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed] = {
    Source.single(System.nanoTime()).flatMapConcat { start =>
      Source
        .single(System.nanoTime()).flatMapConcat { requestStart =>
          Source
            .single(QueryRequest.builder().tableName(tableName).build())
            .via(streamClient.queryFlow(parallelism)).map { response =>
              metricsFunctions.setJournalSequenceDuration(System.nanoTime() - requestStart)
              metricsFunctions.addJournalSequenceCounter(response.count().toLong)
              response
            }
        }
        .map { result =>
          result.itemsAsScala.get.map(_(columnsDefConfig.orderingColumnName).n.toLong)
        }
        .takeWhile(_.nonEmpty)
        .mapConcat(_.toVector)
        .drop(offset)
        .take(limit)
        .withAttributes(logLevels).map { response =>
          metricsFunctions.setJournalSequenceTotalDuration(System.nanoTime() - start)
          metricsFunctions.incrementJournalSequenceTotalCounter()
          response
        }
    }
  }

  override def maxJournalSequence(): Source[Long, NotUsed] =
    Source.single(Long.MaxValue)

}
