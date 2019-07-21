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

import java.io.IOException

import akka.NotUsed
import akka.serialization.Serialization
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source, SourceQueueWithComplete }
import akka.stream.{ Materializer, OverflowStrategy, QueueOfferResult }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, JournalPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PartitionKey, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import monix.execution.Scheduler
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.Success

class WriteJournalDaoImpl(
    asyncClient: DynamoDbAsyncClient,
    serialization: Serialization,
    pluginConfig: JournalPluginConfig,
    protected val metricsReporter: MetricsReporter
)(
    implicit ec: ExecutionContext,
    mat: Materializer
) extends WriteJournalDao
    with DaoSupport {

  import pluginConfig._
  override protected val streamClient: DynamoDbAkkaClient = DynamoDbAkkaClient(asyncClient)

  private implicit val scheduler: Scheduler              = Scheduler(ec)
  override val tableName: String                         = pluginConfig.tableName
  override val getJournalRowsIndexName: String           = pluginConfig.getJournalRowsIndexName
  override val parallelism: Int                          = pluginConfig.parallelism
  override val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig
  private val logger                                     = LoggerFactory.getLogger(getClass)

  private val putQueue: SourceQueueWithComplete[(Promise[Long], Seq[JournalRow])] =
    Source
      .queue[(Promise[Long], Seq[JournalRow])](bufferSize, OverflowStrategy.dropNew)
      .mapAsync(parallelism) {
        case (promise, rows) =>
          if (rows.size > clientConfig.batchWriteItemLimit)
            Source(rows.toVector)
              .grouped(clientConfig.batchWriteItemLimit).log("grouped")
              .via(putJournalRowsFlow).log("put")
              .fold(ArrayBuffer.empty[Long])(_ += _).map(_.sum).log("fold")
              .map(result => promise.success(result))
              .recover { case t => promise.failure(t) }
              .runWith(Sink.ignore)
          else
            Source
              .single(rows)
              .via(putJournalRowsFlow).log("put")
              .map(result => promise.success(result))
              .recover { case t => promise.failure(t) }
              .runWith(Sink.ignore)
      }
      .toMat(Sink.ignore)(Keep.left)
      .withAttributes(logLevels)
      .run()

//  private val putQueues: Seq[SourceQueueWithComplete[(Promise[Long], Seq[JournalRow])]] = for (_ <- 1 to parallelism)
//    yield putQueue
//
//  private def selectPutQueue(persistenceId: PersistenceId): SourceQueueWithComplete[(Promise[Long], Seq[JournalRow])] =
//    putQueues(Math.abs(persistenceId.value.##) % parallelism)

  private val deleteQueue: SourceQueueWithComplete[(Promise[Long], Seq[PersistenceIdWithSeqNr])] =
    Source
      .queue[(Promise[Long], Seq[PersistenceIdWithSeqNr])](bufferSize, OverflowStrategy.dropNew)
      .mapAsync(parallelism) {
        case (promise, rows) =>
          if (rows.size > clientConfig.batchWriteItemLimit)
            Source(rows.toVector)
              .grouped(clientConfig.batchWriteItemLimit).log("grouped")
              .via(deleteJournalRowsFlow).log("delete")
              .fold(ArrayBuffer.empty[Long])(_ += _).map(_.sum).log("fold")
              .map(result => promise.success(result))
              .recover { case t => promise.failure(t) }
              .runWith(Sink.ignore)
          else
            Source
              .single(rows)
              .via(deleteJournalRowsFlow).log("delete")
              .map(result => promise.success(result))
              .recover { case t => promise.failure(t) }
              .runWith(Sink.ignore)
      }
      .toMat(Sink.ignore)(Keep.left)
      .withAttributes(logLevels)
      .run()

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    startTimeSource
      .flatMapConcat { start =>
        Source
          .lazily { () =>
            val updateRequest = UpdateItemRequest
              .builder()
              .tableName(tableName).keyAsScala(
                Map(
                  columnsDefConfig.partitionKeyColumnName -> AttributeValue
                    .builder()
                    .s(journalRow.partitionKey.asString(shardCount)).build(),
                  columnsDefConfig.sequenceNrColumnName -> AttributeValue
                    .builder()
                    .n(journalRow.sequenceNumber.asString).build()
                )
              ).attributeUpdatesAsScala(
                Map(
                  columnsDefConfig.messageColumnName -> AttributeValueUpdate
                    .builder()
                    .action(AttributeAction.PUT).value(
                      AttributeValue.builder().b(SdkBytes.fromByteArray(journalRow.message)).build()
                    ).build(),
                  columnsDefConfig.orderingColumnName ->
                  AttributeValueUpdate
                    .builder()
                    .action(AttributeAction.PUT).value(
                      AttributeValue.builder().n(journalRow.ordering.toString).build()
                    ).build(),
                  columnsDefConfig.deletedColumnName -> AttributeValueUpdate
                    .builder()
                    .action(AttributeAction.PUT).value(
                      AttributeValue.builder().bool(journalRow.deleted).build()
                    ).build()
                ) ++ journalRow.tags
                  .map { tag =>
                    Map(
                      columnsDefConfig.tagsColumnName -> AttributeValueUpdate
                        .builder()
                        .action(AttributeAction.PUT).value(AttributeValue.builder().s(tag).build()).build()
                    )
                  }.getOrElse(Map.empty)
              ).build()
            Source
              .single(updateRequest)
          }.via(streamClient.updateItemFlow(1)).map { _ =>
            ()
          }.map { response =>
            metricsReporter.setUpdateMessageCallDuration(System.nanoTime() - start)
            metricsReporter.incrementUpdateMessageCallCounter()
            response
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setUpdateMessageCallDuration(System.nanoTime() - start)
                metricsReporter.incrementUpdateMessageCallErrorCounter()
                Source.failed(t)
            }
          )
      }.withAttributes(logLevels)
  }

  override def deleteMessages(persistenceId: PersistenceId, toSequenceNr: SequenceNumber): Source[Long, NotUsed] = {
    startTimeSource
      .flatMapConcat { start =>
        getJournalRows(persistenceId, toSequenceNr)
          .flatMapConcat { journalRows =>
            putMessages(journalRows.map(_.withDeleted)).map(result => (result, journalRows))
          }.flatMapConcat {
            case (result, journalRows) =>
              if (!softDeleted) {
                highestSequenceNr(persistenceId, deleted = Some(true))
                  .flatMapConcat { highestMarkedSequenceNr =>
                    getJournalRows(persistenceId, SequenceNumber(highestMarkedSequenceNr - 1)).flatMapConcat { _ =>
                      deleteBy(persistenceId, journalRows.map(_.sequenceNumber))
                    }
                  }
              } else
                Source.single(result)
          }.map { response =>
            metricsReporter.setDeleteMessagesCallDuration(System.nanoTime() - start)
            metricsReporter.incrementDeleteMessagesCallCounter()
            response
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setDeleteMessagesCallDuration(System.nanoTime() - start)
                metricsReporter.incrementDeleteMessagesCallErrorCounter()
                Source.failed(t)
            }
          )
      }.withAttributes(logLevels)
  }

  override def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed] =
    startTimeSource.flatMapConcat { start =>
      if (messages.isEmpty)
        Source.single(0L)
      else
        Source
          .single(messages).via(requestPutJournalRows).map { response =>
            metricsReporter.setPutMessagesCallDuration(System.nanoTime() - start)
            metricsReporter.incrementPutMessagesCallCounter()
            response
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setPutMessagesCallDuration(System.nanoTime() - start)
                metricsReporter.incrementPutMessagesCallErrorCounter()
                Source.failed(t)
            }
          )
    }

  private def requestPutJournalRows: Flow[Seq[JournalRow], Long, NotUsed] =
    Flow[Seq[JournalRow]]
      .mapAsync(1) { messages =>
        val promise = Promise[Long]()
        //selectPutQueue(messages.head.persistenceId)
        putQueue.offer(promise -> messages).flatMap {
          case QueueOfferResult.Enqueued =>
            metricsReporter.addPutMessagesEnqueueCounter(messages.size.toLong)
            val future = promise.future
            future.onComplete {
              case Success(result) =>
                metricsReporter.addPutMessagesDequeueCounter(result)
              case _ =>
            }
            future
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
      }.withAttributes(logLevels)

  private def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean = false
  ): Source[Seq[JournalRow], NotUsed] = {
    startTimeSource
      .flatMapConcat { start =>
        Source
          .lazily { () =>
            val queryRequest = QueryRequest
              .builder()
              .tableName(tableName)
              .indexName(getJournalRowsIndexName)
              .keyConditionExpression("#pid = :pid and #snr <= :snr")
              .filterExpression("#d = :flg")
              .expressionAttributeNamesAsScala(
                Map(
                  "#pid" -> columnsDefConfig.persistenceIdColumnName,
                  "#snr" -> columnsDefConfig.sequenceNrColumnName,
                  "#d"   -> columnsDefConfig.deletedColumnName
                )
              )
              .expressionAttributeValuesAsScala(
                Some(
                  Map(
                    ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
                    ":snr" -> AttributeValue.builder().n(toSequenceNr.asString).build(),
                    ":flg" -> AttributeValue.builder().bool(deleted).build()
                  )
                )
              ).build()
            Source
              .single(queryRequest)
          }
          .via(streamClient.queryFlow())
          .map((_, start))
      }.flatMapConcat {
        case (response, start) =>
          metricsReporter.setGetJournalRowsItemDuration(System.nanoTime() - start)
          if (response.sdkHttpResponse().isSuccessful) {
            metricsReporter.incrementGetJournalRowsItemCallCounter()
            if (response.count() > 0)
              metricsReporter.addGetJournalRowsItemCounter(response.count().toLong)
            Source.single(response)
          } else {
            metricsReporter.incrementGetJournalRowsItemCallErrorCounter()
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
          }
      }
      .mapConcat(_.itemsAsScala.get.toVector)
      .map(convertToJournalRow).fold(ArrayBuffer.empty[JournalRow]) { case (r, e) => r.append(e); r }
      .map(_.result().toVector).withAttributes(logLevels)

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
    Flow[Seq[PersistenceIdWithSeqNr]]
      .mapAsync(parallelism) { messages =>
        val promise = Promise[Long]()
        deleteQueue.offer(promise -> messages).flatMap {
          case QueueOfferResult.Enqueued =>
            metricsReporter.addDeleteMessagesEnqueueCounter(messages.size.toLong)
            val future = promise.future
            future.onComplete {
              case Success(result) =>
                metricsReporter.addDeleteMessagesDequeueCounter(result)
              case _ =>
            }
            future
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
      }.withAttributes(logLevels)

  override def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber
  ): Source[Long, NotUsed] = {
    highestSequenceNr(persistenceId, Some(fromSequenceNr))
  }

  private def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): Source[Long, NotUsed] = {
    startTimeSource.flatMapConcat { callStat =>
      startTimeSource
        .flatMapConcat { itemStart =>
          Source
            .lazily { () =>
              val queryRequest = QueryRequest
                .builder()
                .tableName(tableName)
                .indexName(getJournalRowsIndexName)
                .keyConditionExpressionAsScala(
                  fromSequenceNr.map(_ => "#pid = :id and #snr >= :nr").orElse(Some("#pid = :id"))
                )
                .filterExpressionAsScala(deleted.map(_ => "#d = :flg"))
                .expressionAttributeNamesAsScala(
                  Map(
                    "#pid" -> columnsDefConfig.persistenceIdColumnName
                  ) ++ deleted.map(_ => Map("#d"     -> columnsDefConfig.deletedColumnName)).getOrElse(Map.empty) ++
                  fromSequenceNr.map(_ => Map("#snr" -> columnsDefConfig.sequenceNrColumnName)).getOrElse(Map.empty)
                )
                .expressionAttributeValuesAsScala(
                  Map(
                    ":id" -> AttributeValue.builder().s(persistenceId.asString).build()
                  ) ++ deleted
                    .map(d => Map(":flg" -> AttributeValue.builder().bool(d).build())).getOrElse(Map.empty) ++ fromSequenceNr
                    .map(nr => Map(":nr" -> AttributeValue.builder().n(nr.asString).build())).getOrElse(Map.empty)
                ).scanIndexForward(false)
                .limit(1).build()
              Source
                .single(queryRequest)
            }
            .via(streamClient.queryFlow())
            .map((_, itemStart))
        }
        .flatMapConcat {
          case (response, itemStart) =>
            metricsReporter
              .setHighestSequenceNrItemDuration(System.nanoTime() - itemStart)
            if (response.sdkHttpResponse().isSuccessful) {
              metricsReporter.incrementHighestSequenceNrItemCallCounter()
              if (response.count() > 0)
                metricsReporter.addHighestSequenceNrItemCounter(response.count().toLong)
              Source.single(
                response.itemsAsScala.get
                  .map(_(columnsDefConfig.sequenceNrColumnName).n.toLong).headOption.getOrElse(0L)
              )
            } else {
              metricsReporter.incrementHighestSequenceNrItemCallErrorCounter()
              val statusCode = response.sdkHttpResponse().statusCode()
              val statusText = response.sdkHttpResponse().statusText()
              Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
            }
        }.map { response =>
          metricsReporter.setHighestSequenceNrCallDuration(System.nanoTime() - callStat)
          metricsReporter.incrementHighestSequenceNrCallCounter()
          response
        }.recoverWithRetries(
          1, {
            case t: Throwable =>
              metricsReporter.setHighestSequenceNrCallDuration(System.nanoTime() - callStat)
              metricsReporter.incrementHighestSequenceNrCallErrorCounter()
              Source.failed(t)
          }
        )
        .withAttributes(logLevels)
    }
  }

  private def deleteJournalRowsFlow: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]]
      .flatMapConcat { persistenceIdWithSeqNrs =>
        startTimeSource
          .flatMapConcat { callStart =>
            logger.debug(s"deleteJournalRows.size: ${persistenceIdWithSeqNrs.size}")
            logger.debug(s"deleteJournalRows: $persistenceIdWithSeqNrs")
            persistenceIdWithSeqNrs
              .map { case PersistenceIdWithSeqNr(pid, seqNr) => s"pid = $pid, seqNr = $seqNr" }.foreach(logger.debug)

            def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
              Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
                startTimeSource
                  .flatMapConcat { start =>
                    Source
                      .single(requestItems).map { requests =>
                        BatchWriteItemRequest.builder().requestItemsAsScala(Map(tableName -> requests)).build()
                      }.via(streamClient.batchWriteItemFlow()).map((_, start))
                  }.flatMapConcat {
                    case (response, start) =>
                      metricsReporter.setDeleteJournalRowsItemDuration(System.nanoTime() - start)
                      if (response.sdkHttpResponse().isSuccessful) {
                        metricsReporter.incrementDeleteJournalRowsItemCallCounter()
                        if (response.unprocessedItemsAsScala.get.nonEmpty) {
                          val n = requestItems.size - response.unprocessedItems.get(tableName).size
                          metricsReporter.addDeleteJournalRowsItemCounter(n)
                          Source.single(response.unprocessedItemsAsScala.get(tableName)).via(loopFlow).map(_ + n)
                        } else {
                          metricsReporter.addDeleteJournalRowsItemCounter(requestItems.size)
                          Source.single(requestItems.size)
                        }
                      } else {
                        metricsReporter.incrementDeleteJournalRowsItemCallErrorCounter()
                        val statusCode = response.sdkHttpResponse().statusCode()
                        val statusText = response.sdkHttpResponse().statusText()
                        Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
                      }
                  }
              }

            if (persistenceIdWithSeqNrs.isEmpty)
              Source.single(0L)
            else
              Source
                .lazily { () =>
                  val requestItems = persistenceIdWithSeqNrs.map { persistenceIdWithSeqNr =>
                    WriteRequest
                      .builder().deleteRequest(
                        DeleteRequest
                          .builder().keyAsScala(
                            Map(
                              columnsDefConfig.persistenceIdColumnName -> AttributeValue
                                .builder()
                                .s(persistenceIdWithSeqNr.persistenceId.asString).build(),
                              columnsDefConfig.sequenceNrColumnName -> AttributeValue
                                .builder().n(
                                  persistenceIdWithSeqNr.sequenceNumber.asString
                                ).build()
                            )
                          ).build()
                      ).build()
                  }
                  Source
                    .single(requestItems)
                }.via(loopFlow).map { response =>
                  metricsReporter.setDeleteJournalRowsCallDuration(System.nanoTime() - callStart)
                  metricsReporter.incrementDeleteJournalRowsCallCounter()
                  response
                }.recoverWithRetries(
                  attempts = 1, {
                    case t: Throwable =>
                      metricsReporter.setDeleteJournalRowsCallDuration(System.nanoTime() - callStart)
                      metricsReporter.incrementDeleteJournalRowsCallErrorCounter()
                      Source.failed(t)
                  }
                )
          }
      }.withAttributes(logLevels)

  private def putJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] = Flow[Seq[JournalRow]].flatMapConcat {
    journalRows =>
      startTimeSource
        .flatMapConcat { start =>
          logger.debug(s"putJournalRows.size: ${journalRows.size}")
          logger.debug(s"putJournalRows: $journalRows")
          journalRows.map(_.persistenceId).map(p => s"pid = $p").foreach(logger.debug)

          def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
            Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
              startTimeSource
                .flatMapConcat { start =>
                  Source
                    .single(requestItems).map { requests =>
                      BatchWriteItemRequest.builder().requestItemsAsScala(Map(tableName -> requests)).build
                    }.via(streamClient.batchWriteItemFlow()).map((_, start))
                }.flatMapConcat {
                  case (response, start) =>
                    metricsReporter.setPutJournalRowsItemDuration(System.nanoTime() - start)
                    if (response.sdkHttpResponse().isSuccessful) {
                      metricsReporter.addPutJournalRowsItemCallCounter()
                      if (response.unprocessedItemsAsScala.get.nonEmpty) {
                        val n = requestItems.size - response.unprocessedItems.get(tableName).size
                        metricsReporter.addPutJournalRowsItemCounter(n)
                        Source.single(response.unprocessedItemsAsScala.get(tableName)).via(loopFlow).map(_ + n)
                      } else {
                        metricsReporter.addPutJournalRowsItemCounter(requestItems.size)
                        Source.single(requestItems.size)
                      }
                    } else {
                      metricsReporter.incrementPutJournalRowsItemCallErrorCounter()
                      val statusCode = response.sdkHttpResponse().statusCode()
                      val statusText = response.sdkHttpResponse().statusText()
                      Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
                    }
                }
            }

          if (journalRows.isEmpty)
            Source.single(0L)
          else
            Source
              .lazily { () =>
                val requestItems = journalRows.map { journalRow =>
                  WriteRequest
                    .builder().putRequest(
                      PutRequest
                        .builder()
                        .itemAsScala(
                          Map(
                            columnsDefConfig.partitionKeyColumnName -> AttributeValue
                              .builder()
                              .s(
                                PartitionKey(journalRow.persistenceId, journalRow.sequenceNumber).asString(shardCount)
                              ).build(),
                            columnsDefConfig.persistenceIdColumnName -> AttributeValue
                              .builder()
                              .s(journalRow.persistenceId.asString).build(),
                            columnsDefConfig.sequenceNrColumnName -> AttributeValue
                              .builder()
                              .n(journalRow.sequenceNumber.asString).build(),
                            columnsDefConfig.orderingColumnName -> AttributeValue
                              .builder()
                              .n(journalRow.ordering.toString).build(),
                            columnsDefConfig.deletedColumnName -> AttributeValue
                              .builder().bool(journalRow.deleted).build(),
                            columnsDefConfig.messageColumnName -> AttributeValue
                              .builder().b(SdkBytes.fromByteArray(journalRow.message)).build()
                          ) ++ journalRow.tags
                            .map { tag =>
                              Map(columnsDefConfig.tagsColumnName -> AttributeValue.builder().s(tag).build())
                            }.getOrElse(Map.empty)
                        ).build()
                    ).build()
                }
                Source.single(requestItems)
              }
              .via(loopFlow).map { response =>
                metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - start)
                metricsReporter.incrementPutJournalRowsCallCounter()
                response
              }
              .recoverWithRetries(
                attempts = 1, {
                  case t: Throwable =>
                    metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - start)
                    metricsReporter.incrementPutJournalRowsCallErrorCounter()
                    Source.failed(t)
                }
              )
        }.withAttributes(logLevels)
  }

  case class PersistenceIdWithSeqNr(persistenceId: PersistenceId, sequenceNumber: SequenceNumber)

}
