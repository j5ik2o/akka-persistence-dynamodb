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
package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao

import akka.NotUsed
import akka.serialization.Serialization
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.{ Materializer, OverflowStrategy, QueueOfferResult }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, JournalPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PartitionKey, PersistenceId, SequenceNumber }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDBStreamClient
import com.github.j5ik2o.reactive.aws.dynamodb.model._
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future, Promise }

class WriteJournalDaoImpl(asyncClient: DynamoDBAsyncClientV2,
                          serialization: Serialization,
                          pluginConfig: JournalPluginConfig)(
    implicit ec: ExecutionContext,
    mat: Materializer
) extends WriteJournalDao
    with DaoSupport {

  import pluginConfig._

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val scheduler: Scheduler = Scheduler(ec)

  override protected val streamClient: DynamoDBStreamClient = DynamoDBStreamClient(asyncClient)

  override val tableName: String                         = pluginConfig.tableName
  override val getJournalRowsIndexName: String           = pluginConfig.getJournalRowsIndexName
  override val parallelism: Int                          = pluginConfig.parallelism
  override val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig

  private val putQueue = Source
    .queue[(Promise[Long], Seq[JournalRow])](bufferSize, OverflowStrategy.dropNew)
    .flatMapConcat {
      case (promise, rows) =>
        Source(rows.toVector)
          .grouped(clientConfig.batchWriteItemLimit).log("grouped")
          .via(putJournalRowsFlow).log("result")
          .async
          .fold(ArrayBuffer.empty[Long])(_ :+ _).log("results")
          .map(_.sum).log("sum")
          .map(result => promise.success(result))
          .recover { case t => promise.failure(t) }
    }
    .toMat(Sink.ignore)(Keep.left)
    .withAttributes(logLevels)
    .run()

  private val deleteQueue = Source
    .queue[(Promise[Long], Seq[PersistenceIdWithSeqNr])](bufferSize, OverflowStrategy.dropNew)
    .flatMapConcat {
      case (promise, rows) =>
        Source(rows.toVector)
          .grouped(clientConfig.batchWriteItemLimit).log("grouped")
          .via(deleteJournalRowsFlow).log("result")
          .async
          .fold(ArrayBuffer.empty[Long])(_ :+ _).log("results")
          .map(_.sum).log("sum")
          .map(result => promise.success(result))
          .recover { case t => promise.failure(t) }
    }
    .toMat(Sink.ignore)(Keep.left)
    .withAttributes(logLevels)
    .run()

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    val updateRequest = UpdateItemRequest()
      .withTableName(Some(tableName)).withKey(
        Some(
          Map(
            columnsDefConfig.partitionKeyColumnName -> AttributeValue()
              .withString(Some(journalRow.partitionKey.asString(shardCount))),
            columnsDefConfig.sequenceNrColumnName -> AttributeValue()
              .withNumber(Some(journalRow.sequenceNumber.asString))
          )
        )
      ).withAttributeUpdates(
        Some(
          Map(
            columnsDefConfig.messageColumnName -> AttributeValueUpdate()
              .withAction(Some(AttributeAction.PUT)).withValue(
                Some(AttributeValue().withBinary(Some(journalRow.message)))
              ),
            columnsDefConfig.orderingColumnName ->
            AttributeValueUpdate()
              .withAction(Some(AttributeAction.PUT)).withValue(
                Some(
                  AttributeValue().withNumber(Some(journalRow.ordering.toString))
                )
              ),
            columnsDefConfig.deletedColumnName -> AttributeValueUpdate()
              .withAction(Some(AttributeAction.PUT)).withValue(
                Some(AttributeValue().withBool(Some(journalRow.deleted)))
              )
          ) ++ journalRow.tags
            .map { tag =>
              Map(
                columnsDefConfig.tagsColumnName -> AttributeValueUpdate()
                  .withAction(Some(AttributeAction.PUT)).withValue(Some(AttributeValue().withString(Some(tag))))
              )
            }.getOrElse(Map.empty)
        )
      )
    Source.single(updateRequest).via(streamClient.updateItemFlow(parallelism)).map { _ =>
      ()
    }
  }

  override def deleteMessages(persistenceId: PersistenceId, toSequenceNr: SequenceNumber): Source[Long, NotUsed] = {
    getJournalRows(persistenceId, toSequenceNr)
      .flatMapConcat { journalRows =>
        putMessages(journalRows.map(_.withDeleted)).map(result => (result, journalRows))
      }.flatMapConcat {
        case (result, journalRows) =>
          if (!softDeleted) {
            highestSequenceNr(persistenceId, deleted = Some(true)).flatMapConcat { highestMarkedSequenceNr =>
              getJournalRows(persistenceId, SequenceNumber(highestMarkedSequenceNr - 1)).flatMapConcat { _ =>
                deleteBy(persistenceId, journalRows.map(_.sequenceNumber))
              }
            }
          } else
            Source.single(result)
      }
  }

  override def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed] =
    Source.single(messages).via(requestPutJournalRows)

  private def requestPutJournalRows: Flow[Seq[JournalRow], Long, NotUsed] =
    Flow[Seq[JournalRow]].mapAsync(parallelism) { messages =>
      val promise = Promise[Long]()
      putQueue.offer(promise -> messages).flatMap {
        case QueueOfferResult.Enqueued =>
          promise.future
        case QueueOfferResult.Failure(t) =>
          Future.failed(new Exception("Failed to write journal row batch", t))
        case QueueOfferResult.Dropped =>
          Future.failed(
            new Exception(
              s"Failed to enqueue journal row batch write, the queue buffer was full ($bufferSize elements) please check the jdbc-journal.bufferSize setting"
            )
          )
        case QueueOfferResult.QueueClosed =>
          Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
      }
    }

  private def getJournalRows(persistenceId: PersistenceId,
                             toSequenceNr: SequenceNumber,
                             deleted: Boolean = false): Source[Seq[JournalRow], NotUsed] = {
    val queryRequest = QueryRequest()
      .withTableName(Some(tableName))
      .withIndexName(Some(getJournalRowsIndexName))
      .withKeyConditionExpression(Some("#pid = :pid and #snr <= :snr"))
      .withFilterExpression(Some("#d = :flg"))
      .withExpressionAttributeNames(
        Some(
          Map("#pid" -> columnsDefConfig.persistenceIdColumnName,
              "#snr" -> columnsDefConfig.sequenceNrColumnName,
              "#d"   -> columnsDefConfig.deletedColumnName)
        )
      )
      .withExpressionAttributeValues(
        Some(
          Map(
            ":pid" -> AttributeValue().withString(Some(persistenceId.asString)),
            ":snr" -> AttributeValue().withNumber(Some(toSequenceNr.asString)),
            ":flg" -> AttributeValue().withBool(Some(deleted))
          )
        )
      )
    Source
      .single(queryRequest)
      .via(streamClient.queryFlow())
      .mapConcat(_.items.get.toVector)
      .map(convertToJournalRow).fold(ArrayBuffer.empty[JournalRow]) { case (r, e) => r.append(e); r }
      .map(_.result().toVector)
  }

  private def deleteBy(persistenceId: PersistenceId, sequenceNrs: Seq[SequenceNumber]): Source[Long, NotUsed] = {
    if (sequenceNrs.isEmpty)
      Source.empty
    else {
      Source
        .single(sequenceNrs.map(snr => PersistenceIdWithSeqNr(persistenceId, snr))).via(requestDeleteJournalRows)
    }
  }

  private def requestDeleteJournalRows: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]].mapAsync(parallelism) { messages =>
      val promise = Promise[Long]()
      deleteQueue.offer(promise -> messages).flatMap {
        case QueueOfferResult.Enqueued =>
          promise.future
        case QueueOfferResult.Failure(t) =>
          Future.failed(new Exception("Failed to write journal row batch", t))
        case QueueOfferResult.Dropped =>
          Future.failed(
            new Exception(
              s"Failed to enqueue journal row batch write, the queue buffer was full ($bufferSize elements) please check the jdbc-journal.bufferSize setting"
            )
          )
        case QueueOfferResult.QueueClosed =>
          Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
      }
    }

  private def highestSequenceNr(persistenceId: PersistenceId,
                                fromSequenceNr: Option[SequenceNumber] = None,
                                deleted: Option[Boolean] = None): Source[Long, NotUsed] = {
    val queryRequest = QueryRequest()
      .withTableName(Some(tableName))
      .withIndexName(Some(getJournalRowsIndexName))
      .withKeyConditionExpression(
        fromSequenceNr.map(_ => "#pid = :id and #snr >= :nr").orElse(Some("#pid = :id"))
      )
      .withFilterExpression(deleted.map(_ => "#d = :flg"))
      .withExpressionAttributeNames(
        Some(
          Map(
            "#pid" -> columnsDefConfig.persistenceIdColumnName
          ) ++ deleted.map(_ => Map("#d"     -> columnsDefConfig.deletedColumnName)).getOrElse(Map.empty) ++
          fromSequenceNr.map(_ => Map("#snr" -> columnsDefConfig.sequenceNrColumnName)).getOrElse(Map.empty)
        )
      )
      .withExpressionAttributeValues(
        Some(
          Map(
            ":id" -> AttributeValue().withString(Some(persistenceId.asString))
          ) ++ deleted
            .map(d => Map(":flg" -> AttributeValue().withBool(Some(d)))).getOrElse(Map.empty) ++ fromSequenceNr
            .map(nr => Map(":nr" -> AttributeValue().withNumber(Some(nr.asString)))).getOrElse(Map.empty)
        )
      ).withScanIndexForward(Some(false))
      .withLimit(Some(1))
    Source
      .single(queryRequest).via(streamClient.queryFlow()).map {
        _.items.get.map(_(columnsDefConfig.sequenceNrColumnName).number.get.toLong).headOption.getOrElse(0)
      }

  }

  override def highestSequenceNr(persistenceId: PersistenceId,
                                 fromSequenceNr: SequenceNumber): Source[Long, NotUsed] = {
    highestSequenceNr(persistenceId, Some(fromSequenceNr))
  }

  private def deleteJournalRowsFlow: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]].flatMapConcat { persistenceIdWithSeqNrs =>
      logger.debug(s"deleteJournalRows.size: ${persistenceIdWithSeqNrs.size}")
      logger.debug(s"deleteJournalRows: $persistenceIdWithSeqNrs")
      persistenceIdWithSeqNrs
        .map { case PersistenceIdWithSeqNr(pid, seqNr) => s"pid = $pid, seqNr = $seqNr" }.foreach(logger.debug)

      def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
        Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
          if (requestItems.isEmpty)
            Source.single(0L)
          else {
            Source
              .single(requestItems).map { requests =>
                BatchWriteItemRequest().withRequestItems(Some(Map(tableName -> requests)))
              }.via(streamClient.batchWriteItemFlow()).flatMapConcat { response =>
                if (response.unprocessedItems.get.nonEmpty) {
                  val n = requestItems.size - response.unprocessedItems.get(tableName).size
                  Source.single(response.unprocessedItems.get(tableName)).via(loopFlow).map(_ + n)
                } else
                  Source.single(requestItems.size)
              }
          }
        }

      val requestItems = persistenceIdWithSeqNrs.map { persistenceIdWithSeqNr =>
        WriteRequest().withDeleteRequest(
          Some(
            DeleteRequest().withKey(
              Some(
                Map(
                  columnsDefConfig.persistenceIdColumnName -> AttributeValue()
                    .withString(Some(persistenceIdWithSeqNr.persistenceId.asString)),
                  columnsDefConfig.sequenceNrColumnName -> AttributeValue().withNumber(
                    Some(persistenceIdWithSeqNr.sequenceNumber.asString)
                  )
                )
              )
            )
          )
        )
      }
      Source.single(requestItems).via(loopFlow)
    }

  private def putJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] = Flow[Seq[JournalRow]].flatMapConcat {
    journalRows =>
      logger.debug(s"putJournalRows.size: ${journalRows.size}")
      logger.debug(s"putJournalRows: $journalRows")
      journalRows.map(_.persistenceId).map(p => s"pid = $p").foreach(logger.debug)

      def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
        Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
          if (requestItems.isEmpty)
            Source.single(0L)
          else {
            Source
              .single(requestItems).map { requests =>
                BatchWriteItemRequest().withRequestItems(Some(Map(tableName -> requests)))
              }.via(streamClient.batchWriteItemFlow()).flatMapConcat { response =>
                if (response.unprocessedItems.get.nonEmpty) {
                  val n = requestItems.size - response.unprocessedItems.get(tableName).size
                  Source.single(response.unprocessedItems.get(tableName)).via(loopFlow).map(_ + n)
                } else
                  Source.single(requestItems.size)
              }
          }
        }

      val requestItems = journalRows.map { journalRow =>
        WriteRequest().withPutRequest(
          Some(
            PutRequest()
              .withItem(
                Some(
                  Map(
                    columnsDefConfig.partitionKeyColumnName -> AttributeValue()
                      .withString(
                        Some(PartitionKey(journalRow.persistenceId, journalRow.sequenceNumber).asString(shardCount))
                      ),
                    columnsDefConfig.persistenceIdColumnName -> AttributeValue()
                      .withString(Some(journalRow.persistenceId.asString)),
                    columnsDefConfig.sequenceNrColumnName -> AttributeValue()
                      .withNumber(Some(journalRow.sequenceNumber.asString)),
                    columnsDefConfig.orderingColumnName -> AttributeValue()
                      .withNumber(Some(journalRow.ordering.toString)),
                    columnsDefConfig.deletedColumnName -> AttributeValue().withBool(Some(journalRow.deleted)),
                    columnsDefConfig.messageColumnName -> AttributeValue().withBinary(Some(journalRow.message))
                  ) ++ journalRow.tags
                    .map { tag =>
                      Map(columnsDefConfig.tagsColumnName -> AttributeValue().withString(Some(tag)))
                    }.getOrElse(Map.empty)
                )
              )
          )
        )
      }
      Source.single(requestItems).via(loopFlow)
  }

  case class PersistenceIdWithSeqNr(persistenceId: PersistenceId, sequenceNumber: SequenceNumber)

}
