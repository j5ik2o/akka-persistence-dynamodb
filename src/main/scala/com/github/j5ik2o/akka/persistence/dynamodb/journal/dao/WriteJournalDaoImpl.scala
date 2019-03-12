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
import akka.stream.{ Attributes, Materializer, OverflowStrategy, QueueOfferResult }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
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
) extends WriteJournalDao {

  import pluginConfig._

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val scheduler: Scheduler = Scheduler(ec)

  private val streamClient: DynamoDBStreamClient = DynamoDBStreamClient(asyncClient)

  private val logLevels = Attributes.logLevels(onElement = Attributes.LogLevels.Debug,
                                               onFailure = Attributes.LogLevels.Error,
                                               onFinish = Attributes.LogLevels.Debug)
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

  override def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed] =
    Source.single(messages).via(requestPutJournalRows)

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    val updateRequest = UpdateItemRequest()
      .withTableName(Some(tableName)).withKey(
        Some(
          Map(
            columnsDefConfig.persistenceIdColumnName -> AttributeValue()
              .withString(Some(journalRow.persistenceId.asString)),
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

  private def getJournalRows(persistenceId: PersistenceId,
                             toSequenceNr: SequenceNumber,
                             deleted: Boolean = false): Source[Seq[JournalRow], NotUsed] = {
    val queryRequest = QueryRequest()
      .withTableName(Some(tableName))
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

  private def highestSequenceNr(persistenceId: PersistenceId,
                                fromSequenceNr: Option[SequenceNumber] = None,
                                deleted: Option[Boolean] = None): Source[Long, NotUsed] = {
    val queryRequest = QueryRequest()
      .withTableName(Some(tableName))
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

  override def getMessages(persistenceId: PersistenceId,
                           fromSequenceNr: SequenceNumber,
                           toSequenceNr: SequenceNumber,
                           max: Long,
                           deleted: Option[Boolean] = Some(false)): Source[JournalRow, NotUsed] = {
    Source
      .unfold(fromSequenceNr.value) {
        case nr if nr > toSequenceNr.value =>
          None
        case nr =>
          Some(nr + 1, nr)
      }
      .grouped(clientConfig.batchGetItemLimit).log("grouped")
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
                          columnsDefConfig.persistenceIdColumnName -> AttributeValue().withString(
                            Some(persistenceId.asString)
                          ),
                          columnsDefConfig.sequenceNrColumnName -> AttributeValue().withNumber(Some(seqNr.toString))
                        )
                      }
                    )
                  )
              )
            )
          )
      }
      .via(streamClient.batchGetItemFlow())
      .map { batchGetItemResponse =>
        batchGetItemResponse.responses.getOrElse(Map.empty)
      }
      .mapConcat { result =>
        (deleted match {
          case None =>
            convertToJournalRows(result(tableName))
          case Some(b) =>
            convertToJournalRows(result(tableName)).filter(_.deleted == b)
        }).sortBy(_.sequenceNumber).toVector
      }.log("journalRow")
      .take(max)
      .withAttributes(logLevels)

  }

  private def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    import com.github.j5ik2o.akka.persistence.dynamodb.journal.PersistenceId
    JournalRow(
      persistenceId = PersistenceId(map(columnsDefConfig.persistenceIdColumnName).string.get),
      sequenceNumber = SequenceNumber(map(columnsDefConfig.sequenceNrColumnName).number.get.toLong),
      deleted = map(columnsDefConfig.deletedColumnName).bool.get,
      message = map.get(columnsDefConfig.messageColumnName).flatMap(_.binary).get,
      ordering = map(columnsDefConfig.orderingColumnName).number.get.toLong,
      tags = map.get(columnsDefConfig.tagsColumnName).flatMap(_.string)
    )

  }

  private def convertToJournalRows(values: Seq[Map[String, AttributeValue]]): Seq[JournalRow] = {
    values.map(convertToJournalRow)
  }

  case class PersistenceIdWithSeqNr(persistenceId: PersistenceId, sequenceNumber: SequenceNumber)

  private def deleteJournalRowsFlow: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]].flatMapConcat { xs =>
      logger.debug(s"deleteJournalRows.size: ${xs.size}")
      logger.debug(s"deleteJournalRows: $xs")
      xs.map { case PersistenceIdWithSeqNr(pid, seqNr) => s"pid = $pid, seqNr = $seqNr" }.foreach(logger.debug)
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
      val requestItems = xs.map { x =>
        WriteRequest().withDeleteRequest(
          Some(
            DeleteRequest().withKey(
              Some(
                Map(
                  columnsDefConfig.persistenceIdColumnName -> AttributeValue()
                    .withString(Some(x.persistenceId.asString)),
                  columnsDefConfig.sequenceNrColumnName -> AttributeValue().withNumber(Some(x.sequenceNumber.asString))
                )
              )
            )
          )
        )
      }
      Source.single(requestItems).via(loopFlow)
    }

  private def putJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] = Flow[Seq[JournalRow]].flatMapConcat { xs =>
    logger.debug(s"putJournalRows.size: ${xs.size}")
    logger.debug(s"putJournalRows: $xs")
    xs.map(_.persistenceId).map(p => s"pid = $p").foreach(logger.debug)
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
    val requestItems = xs.map { x =>
      WriteRequest().withPutRequest(
        Some(
          PutRequest()
            .withItem(
              Some(
                Map(
                  columnsDefConfig.persistenceIdColumnName -> AttributeValue()
                    .withString(Some(x.persistenceId.asString)),
                  columnsDefConfig.sequenceNrColumnName -> AttributeValue().withNumber(Some(x.sequenceNumber.asString)),
                  columnsDefConfig.orderingColumnName   -> AttributeValue().withNumber(Some(x.ordering.toString)),
                  columnsDefConfig.deletedColumnName    -> AttributeValue().withBool(Some(x.deleted)),
                  columnsDefConfig.messageColumnName    -> AttributeValue().withBinary(Some(x.message))
                ) ++ x.tags
                  .map { t =>
                    Map(columnsDefConfig.tagsColumnName -> AttributeValue().withString(Some(t)))
                  }.getOrElse(Map.empty)
              )
            )
        )
      )
    }
    Source.single(requestItems).via(loopFlow)
  }

}
