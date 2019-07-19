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

  private val deleteQueue: SourceQueueWithComplete[(Promise[Long], Seq[PersistenceIdWithSeqNr])] =
    Source
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
      .single(System.nanoTime()).flatMapConcat { start =>
        Source
          .single(updateRequest).via(streamClient.updateItemFlow(parallelism)).map { _ =>
            ()
          }.map { response =>
            metricsReporter.setUpdateMessageDuration(System.nanoTime() - start)
            metricsReporter.incrementUpdateMessageCounter()
            response
          }
      }
  }

  override def deleteMessages(persistenceId: PersistenceId, toSequenceNr: SequenceNumber): Source[Long, NotUsed] = {
    Source.single(System.nanoTime()).flatMapConcat { start =>
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
        }.map { response =>
          metricsReporter.setDeleteMessageDuration(System.nanoTime() - start)
          metricsReporter.incrementDeleteMessageCounter()
          response
        }
    }
  }

  override def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed] =
    Source.single(System.nanoTime()).flatMapConcat { start =>
      Source.single(messages).via(requestPutJournalRows).map { response =>
        metricsReporter.setPutMessageDuration(System.nanoTime() - start)
        metricsReporter.incrementPutMessageCounter()
        response
      }
    }

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

  private def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean = false
  ): Source[Seq[JournalRow], NotUsed] = {
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
      .single(System.nanoTime()).flatMapConcat { start =>
        Source
          .single(queryRequest)
          .via(streamClient.queryFlow())
          .mapConcat(_.itemsAsScala.get.toVector)
          .map(convertToJournalRow).fold(ArrayBuffer.empty[JournalRow]) { case (r, e) => r.append(e); r }
          .map { response =>
            metricsReporter.setGetJournalRowsDuration(System.nanoTime() - start)
            metricsReporter.incrementGetJournalRowsCounter()
            response
          }
      }
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
      .single(System.nanoTime()).flatMapConcat { start =>
        Source
          .single(queryRequest).via(streamClient.queryFlow()).map {
            _.itemsAsScala.get.map(_(columnsDefConfig.sequenceNrColumnName).n.toLong).headOption.getOrElse(0L)
          }.map { response =>
            metricsReporter
              .setHighestSequenceNrTotalDuration(System.nanoTime() - start)
            metricsReporter.incrementHighestSequenceNrTotalCounter()
            response
          }
      }

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
              .single(System.nanoTime()).flatMapConcat { start =>
                Source
                  .single(requestItems).map { requests =>
                    BatchWriteItemRequest.builder().requestItemsAsScala(Map(tableName -> requests)).build()
                  }.via(streamClient.batchWriteItemFlow()).map { response =>
                    metricsReporter.setDeleteJournalRowsDuration(System.nanoTime() - start)
                    metricsReporter.addDeleteJournalRowsCounter(requestItems.size - response.unprocessedItems().size)
                    response
                  }
              }.flatMapConcat { response =>
                if (response.unprocessedItemsAsScala.get.nonEmpty) {
                  val n = requestItems.size - response.unprocessedItems.get(tableName).size
                  Source.single(response.unprocessedItemsAsScala.get(tableName)).via(loopFlow).map(_ + n)
                } else
                  Source.single(requestItems.size)
              }
          }
        }

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
        .single(System.nanoTime()).flatMapConcat { start =>
          Source.single(requestItems).via(loopFlow).map { response =>
            metricsReporter.setDeleteJournalRowsTotalDuration(System.nanoTime() - start)
            metricsReporter.incrementDeleteJournalRowsTotalCounter()
            response
          }
        }
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
              .single(System.nanoTime()).flatMapConcat { start =>
                Source
                  .single(requestItems).map { requests =>
                    BatchWriteItemRequest.builder().requestItemsAsScala(Map(tableName -> requests)).build
                  }.via(streamClient.batchWriteItemFlow()).map { response =>
                    metricsReporter.setPutJournalRowsDuration(System.nanoTime() - start)
                    metricsReporter.addPutJournalRowsCounter(requestItems.size - response.unprocessedItems().size())
                    response
                  }
              }.flatMapConcat { response =>
                if (response.unprocessedItemsAsScala.get.nonEmpty) {
                  val n = requestItems.size - response.unprocessedItems.get(tableName).size
                  Source.single(response.unprocessedItemsAsScala.get(tableName)).via(loopFlow).map(_ + n)
                } else
                  Source.single(requestItems.size)
              }
          }
        }

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
                  columnsDefConfig.deletedColumnName -> AttributeValue.builder().bool(journalRow.deleted).build(),
                  columnsDefConfig.messageColumnName -> AttributeValue
                    .builder().b(SdkBytes.fromByteArray(journalRow.message)).build()
                ) ++ journalRow.tags
                  .map { tag =>
                    Map(columnsDefConfig.tagsColumnName -> AttributeValue.builder().s(tag).build())
                  }.getOrElse(Map.empty)
              ).build()
          ).build()
      }
      Source
        .single(System.nanoTime()).flatMapConcat { start =>
          Source.single(requestItems).via(loopFlow).map { response =>
            metricsReporter.setPutJournalRowsTotalDuration(System.nanoTime() - start)
            metricsReporter.incrementPutJournalRowsTotalCounter()
            response
          }
        }
  }

  case class PersistenceIdWithSeqNr(persistenceId: PersistenceId, sequenceNumber: SequenceNumber)

}
