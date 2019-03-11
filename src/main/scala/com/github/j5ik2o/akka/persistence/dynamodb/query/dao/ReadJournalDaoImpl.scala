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
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.config.QueryPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDBStreamClient
import com.github.j5ik2o.reactive.aws.dynamodb.model._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future }

class ReadJournalDaoImpl(asyncClient: DynamoDBAsyncClientV2,
                         serialization: Serialization,
                         pluginConfig: QueryPluginConfig)(implicit ec: ExecutionContext)
    extends ReadJournalDao {
  import pluginConfig._
  private val logger = LoggerFactory.getLogger(getClass)

  private val logLevels = Attributes.logLevels(onElement = Attributes.LogLevels.Debug,
                                               onFailure = Attributes.LogLevels.Error,
                                               onFinish = Attributes.LogLevels.Debug)

  private val streamClient: DynamoDBStreamClient = DynamoDBStreamClient(asyncClient)

  private def scan(lastKey: Option[Map[String, AttributeValue]]): Future[ScanResponse] = {
    asyncClient.scan(
      ScanRequest()
        .withTableName(Some(tableName))
        .withSelect(Some(Select.ALL_ATTRIBUTES))
        .withLimit(Some(batchSize))
        .withExclusiveStartKey(lastKey)
    )
  }

  override def allPersistenceIdsSource(max: Long): Source[String, NotUsed] = {
    logger.debug("allPersistenceIdsSource: max = {}", max)
    type State = Option[Map[String, AttributeValue]]
    type Elm   = Seq[Map[String, AttributeValue]]
    Source
      .unfoldAsync[Option[State], Elm](None) {
        case None =>
          scan(None)
            .map { v =>
              if (v.lastEvaluatedKey.isEmpty) Some(None, v.items.get)
              else Some(Some(v.lastEvaluatedKey), v.items.get)
            }
        case Some(Some(lastKey)) if lastKey.nonEmpty =>
          scan(Some(lastKey))
            .map { v =>
              if (v.lastEvaluatedKey.isEmpty) Some(None, v.items.get)
              else Some(Some(v.lastEvaluatedKey), v.items.get)
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
        map(columnsDefConfig.persistenceIdColumnName).string.get
      }
      .fold(Set.empty[String]) { case (r, e) => r + e }
      .mapConcat(_.toVector)
      .take(max)
      .withAttributes(logLevels)
  }

  override def eventsByTag(tag: String, offset: Long, maxOffset: Long, max: Long): Source[JournalRow, NotUsed] = {
    val request = ScanRequest()
      .withTableName(Some(tableName))
      .withIndexName(Some("TagsIndex"))
      .withFilterExpression(Some("contains(#tags, :tag)"))
      .withExpressionAttributeNames(
        Some(
          Map("#tags" -> columnsDefConfig.tagsColumnName)
        )
      )
      .withExpressionAttributeValues(
        Some(
          Map(
            ":tag" -> AttributeValue().withString(Some(tag))
          )
        )
      )
    Source
      .single(request).via(streamClient.scanFlow(parallelism)).map { response =>
        response.items.getOrElse(Seq.empty)
      }
      .takeWhile(_.nonEmpty)
      .mapConcat(_.toVector)
      .map(convertToJournalRow)
      .fold(ArrayBuffer.empty[JournalRow]) { case (r, e) => r append e; r }
      .map(_.sortBy(v => (v.persistenceId, v.sequenceNumber)))
      .mapConcat(_.toVector)
      .statefulMapConcat { () =>
        val index = new AtomicLong()
        journalRow =>
          List(journalRow.withOrdering(index.incrementAndGet()))
      }
      .filter { row =>
        row.ordering > offset && row.ordering <= maxOffset
      }
      .take(max)
      .withAttributes(logLevels)
  }

  override def getMessages(persistenceId: String,
                           fromSequenceNr: Long,
                           toSequenceNr: Long,
                           max: Long,
                           deleted: Option[Boolean] = Some(false)): Source[JournalRow, NotUsed] = {
    Source
      .unfold(fromSequenceNr) {
        case nr if nr > toSequenceNr =>
          None
        case nr =>
          Some(nr + 1, nr)
      }
      .grouped(batchSize).log("grouped")
      .map { seqNos =>
        BatchGetItemRequest()
          .withRequestItems(
            Some(
              Map(
                tableName -> KeysAndAttributes()
                  .withKeys(
                    Some(
                      seqNos.map { seqNr =>
                        Map(
                          columnsDefConfig.persistenceIdColumnName -> AttributeValue().withString(Some(persistenceId)),
                          columnsDefConfig.sequenceNrColumnName    -> AttributeValue().withNumber(Some(seqNr.toString))
                        )
                      }
                    )
                  )
              )
            )
          )
      }
      .via(streamClient.batchGetItemFlow(parallelism))
      .map { batchGetItemResponse =>
        batchGetItemResponse.responses.getOrElse(Map.empty)(tableName).toVector
      }
      .takeWhile(_.nonEmpty)
      .mapConcat(_.toVector)
      .map(convertToJournalRow)
      .fold(ArrayBuffer.empty[JournalRow]) { case (r, e) => r append e; r }
      .map(_.sortBy(v => (v.persistenceId, v.sequenceNumber)))
      .mapConcat(_.toVector)
      .statefulMapConcat { () =>
        val index = new AtomicLong()
        journalRow =>
          List(journalRow.withOrdering(index.incrementAndGet()))
      }
      .filter(_.deleted == false).log("journalRow")
      .take(max)
      .withAttributes(logLevels)

  }

  private def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = map(columnsDefConfig.persistenceIdColumnName).string.get,
      sequenceNumber = map(columnsDefConfig.sequenceNrColumnName).number.get.toLong,
      deleted = map(columnsDefConfig.deletedColumnName).bool.get,
      message = map.get(columnsDefConfig.messageColumnName).flatMap(_.binary).get,
      ordering = map(columnsDefConfig.orderingColumnName).number.get.toLong,
      tags = map.get(columnsDefConfig.tagsColumnName).flatMap(_.string)
    )
  }

  override def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed] = {
    Source
      .single(QueryRequest().withTableName(Some(tableName)))
      .via(streamClient.queryFlow(parallelism))
      .map { result =>
        result.items.get.map(_(columnsDefConfig.orderingColumnName).number.get.toLong)
      }
      .takeWhile(_.nonEmpty)
      .mapConcat(_.toVector)
      .drop(offset)
      .take(limit)
      .withAttributes(logLevels)
  }

  override def maxJournalSequence(): Source[Long, NotUsed] =
    Source.single(Long.MaxValue)

}
