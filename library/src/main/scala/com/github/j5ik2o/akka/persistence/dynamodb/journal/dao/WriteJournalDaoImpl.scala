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
import akka.actor.ActorSystem
import akka.serialization.Serialization
import akka.stream.scaladsl.{ Concat, Flow, Keep, Sink, Source, SourceQueueWithComplete, SourceUtils }
import akka.stream.scaladsl.{ Concat, Flow, Keep, Sink, Source, SourceQueueWithComplete }
import akka.stream.{ ActorMaterializer, OverflowStrategy, QueueOfferResult }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, JournalPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PartitionKey, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer
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
    val serializer: FlowPersistentReprSerializer[JournalRow],
    protected val metricsReporter: MetricsReporter
)(
    implicit val ec: ExecutionContext,
    system: ActorSystem
) extends JournalDaoWithUpdates
    with DaoSupport {

  implicit val mat = ActorMaterializer()
  import pluginConfig._

  override protected val streamClient: DynamoDbAkkaClient = DynamoDbAkkaClient(asyncClient)
  private implicit val scheduler: Scheduler               = Scheduler(ec)

  override protected val shardCount: Int                           = pluginConfig.shardCount
  override protected val tableName: String                         = pluginConfig.tableName
  override protected val getJournalRowsIndexName: String           = pluginConfig.getJournalRowsIndexName
  private val queueBufferSize                                      = pluginConfig.queueBufferSize
  private val queueParallelism: Int                                = pluginConfig.queueParallelism
  private val writeParallelism: Int                                = pluginConfig.writeParallelism
  override protected val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig
  override protected val queryBatchSize: Int                       = pluginConfig.queryBatchSize
  override protected val scanBatchSize: Int                        = pluginConfig.scanBatchSize
  override protected val consistentRead: Boolean                   = pluginConfig.consistentRead
  private val logger                                               = LoggerFactory.getLogger(getClass)

  private val queueOverflowStrategy = pluginConfig.queueOverflowStrategy.toLowerCase() match {
    case s if s == OverflowStrategy.dropHead.getClass.getSimpleName.toLowerCase()     => OverflowStrategy.dropHead
    case s if s == OverflowStrategy.dropTail.getClass.getSimpleName.toLowerCase()     => OverflowStrategy.dropTail
    case s if s == OverflowStrategy.dropBuffer.getClass.getSimpleName.toLowerCase()   => OverflowStrategy.dropBuffer
    case s if s == OverflowStrategy.dropNew.getClass.getSimpleName.toLowerCase()      => OverflowStrategy.dropNew
    case s if s == OverflowStrategy.fail.getClass.getSimpleName.toLowerCase()         => OverflowStrategy.fail
    case s if s == OverflowStrategy.backpressure.getClass.getSimpleName.toLowerCase() => OverflowStrategy.backpressure
    case _                                                                            => throw new IllegalArgumentException()
  }

  private def putQueue: SourceQueueWithComplete[(Promise[Long], Seq[JournalRow])] =
    Source
      .queue[(Promise[Long], Seq[JournalRow])](queueBufferSize, queueOverflowStrategy)
      .mapAsync(writeParallelism) {
        case (promise, rows) =>
          logger.debug(s"put rows.size = ${rows.size}")
          if (rows.size == 1)
            Source
              .single(rows.head).via(singlePutJournalRowFlow).log("put")
              .map(result => promise.success(result))
              .recover { case t => promise.failure(t) }
              .runWith(Sink.ignore)
          else if (rows.size > clientConfig.batchWriteItemLimit)
            Source(rows.toVector)
              .grouped(clientConfig.batchWriteItemLimit).log("grouped")
              .via(putJournalRowsFlow).log("put")
              .fold(0L)(_ + _).log("fold")
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

  private val putQueues = for (_ <- 1 to queueParallelism) yield putQueue

  private def queueIdFrom(persistenceId: PersistenceId): Int = Math.abs(persistenceId.value.##) % queueParallelism

  private def selectPutQueue(persistenceId: PersistenceId): SourceQueueWithComplete[(Promise[Long], Seq[JournalRow])] =
    putQueues(queueIdFrom(persistenceId))

  private def deleteQueue: SourceQueueWithComplete[(Promise[Long], Seq[PersistenceIdWithSeqNr])] =
    Source
      .queue[(Promise[Long], Seq[PersistenceIdWithSeqNr])](queueBufferSize, queueOverflowStrategy)
      .mapAsync(writeParallelism) {
        case (promise, rows) =>
          logger.debug(s"delete rows.size = ${rows.size}")
          if (rows.size == 1)
            Source
              .single(rows.head).via(singleDeleteJournalRowFlow).log("delete")
              .map(result => promise.success(result))
              .recover { case t => promise.failure(t) }
              .runWith(Sink.ignore)
          else if (rows.size > clientConfig.batchWriteItemLimit)
            Source(rows.toVector)
              .grouped(clientConfig.batchWriteItemLimit).log("grouped")
              .via(deleteJournalRowsFlow).log("delete")
              .fold(0L)(_ + _).log("fold")
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

  private val deleteQueues = for (_ <- 1 to queueParallelism) yield deleteQueue

  private def selectDeleteQueue(
      persistenceId: PersistenceId
  ): SourceQueueWithComplete[(Promise[Long], Seq[PersistenceIdWithSeqNr])] = deleteQueues(queueIdFrom(persistenceId))

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    startTimeSource
      .flatMapConcat { callStart =>
        logger.debug(s"updateMessage(journalRow = $journalRow): start")
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
          .via(streamClient.updateItemFlow(1)).flatMapConcat { response =>
            if (response.sdkHttpResponse().isSuccessful) {
              Source.single(())
            } else {
              val statusCode = response.sdkHttpResponse().statusCode()
              val statusText = response.sdkHttpResponse().statusText()
              logger.debug(s"updateMessage(journalRow = $journalRow): finished")
              Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
            }
          }.map { response =>
            metricsReporter.setUpdateMessageCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementUpdateMessageCallCounter()
            logger.debug(s"updateMessage(journalRow = $journalRow): finished")
            response
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setUpdateMessageCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementUpdateMessageCallErrorCounter()
                logger.debug(s"updateMessage(journalRow = $journalRow): finished")
                Source.failed(t)
            }
          )
      }.withAttributes(logLevels)
  }

  override def deleteMessages(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber
  ): Source[Long, NotUsed] = {
    startTimeSource
      .flatMapConcat { callStart =>
        logger.debug(s"deleteMessages(persistenceId = $persistenceId, toSequenceNr = $toSequenceNr): start")
        getJournalRows(persistenceId, toSequenceNr, deleted = false)
          .flatMapConcat { journalRows =>
            putMessages(journalRows.map(_.withDeleted)).map(result => (result, journalRows))
          }.flatMapConcat {
            case (result, journalRows) =>
              if (!softDeleted) {
                highestSequenceNr(persistenceId, deleted = Some(true))
                  .flatMapConcat { highestMarkedSequenceNr =>
                    getJournalRows(
                      persistenceId,
                      SequenceNumber(highestMarkedSequenceNr - 1),
                      deleted = false
                    ).flatMapConcat { _ => deleteBy(persistenceId, journalRows.map(_.sequenceNumber)) }
                  }
              } else
                Source.single(result)
          }.map { response =>
            metricsReporter.setDeleteMessagesCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementDeleteMessagesCallCounter()
            logger.debug(s"deleteMessages(persistenceId = $persistenceId, toSequenceNr = $toSequenceNr): finished")
            response
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setDeleteMessagesCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementDeleteMessagesCallErrorCounter()
                logger.debug(s"deleteMessages(persistenceId = $persistenceId, toSequenceNr = $toSequenceNr): finished")
                Source.failed(t)
            }
          )
      }.withAttributes(logLevels)
  }

  override def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed] =
    startTimeSource.flatMapConcat { callStart =>
      logger.debug(s"putMessages(messages = $messages): start")
      if (messages.isEmpty)
        Source.single(0L)
      else
        Source
          .single(messages).via(requestPutJournalRows).map { response =>
            metricsReporter.setPutMessagesCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementPutMessagesCallCounter()
            logger.debug(s"putMessages(messages = $messages): finished")
            response
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setPutMessagesCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementPutMessagesCallErrorCounter()
                logger.debug(s"putMessages(messages = $messages): finished")
                Source.failed(t)
            }
          )
    }

  override def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber
  ): Source[Long, NotUsed] = {
    highestSequenceNr(persistenceId, Some(fromSequenceNr))
  }

  private def requestPutJournalRows: Flow[Seq[JournalRow], Long, NotUsed] =
    Flow[Seq[JournalRow]]
      .mapAsync(1) { messages =>
        val promise = Promise[Long]()
        selectPutQueue(messages.head.persistenceId).offer(promise -> messages).flatMap {
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
                s"Failed to enqueue journal row batch write, the queue buffer was full ($queueBufferSize elements) please check the jdbc-journal.bufferSize setting"
              )
            )
          case QueueOfferResult.QueueClosed =>
            Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
        }
      }.withAttributes(logLevels)

  private def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): Source[Seq[JournalRow], NotUsed] = {
    if (consistentRead) require(shardCount == 1)
    startTimeSource
      .flatMapConcat { callStart =>
        logger.debug(
          s"getJournalRows(persistenceId = $persistenceId, toSequenceNr = $toSequenceNr, deleted = $deleted): start"
        )
        def createNonGSIRequest(lastEvaluatedKey: Option[Map[String, AttributeValue]]): QueryRequest = {
          QueryRequest
            .builder()
            .tableName(tableName)
            .keyConditionExpression("#pid = :pid and #snr <= :snr")
            .filterExpression("#d = :flg")
            .expressionAttributeNamesAsScala(
              Map(
                "#pid" -> columnsDefConfig.partitionKeyColumnName,
                "#snr" -> columnsDefConfig.sequenceNrColumnName,
                "#d"   -> columnsDefConfig.deletedColumnName
              )
            )
            .expressionAttributeValuesAsScala(
              Some(
                Map(
                  ":pid" -> AttributeValue.builder().s(persistenceId.asString + "-0").build(),
                  ":snr" -> AttributeValue.builder().n(toSequenceNr.asString).build(),
                  ":flg" -> AttributeValue.builder().bool(deleted).build()
                )
              )
            )
            .limit(queryBatchSize)
            .exclusiveStartKeyAsScala(lastEvaluatedKey)
            .consistentRead(consistentRead)
            .build()
        }
        def createGSIRequest(lastEvaluatedKey: Option[Map[String, AttributeValue]]): QueryRequest = {
          QueryRequest
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
            )
            .limit(queryBatchSize)
            .exclusiveStartKeyAsScala(lastEvaluatedKey)
            .build()
        }
        def loop(
            lastEvaluatedKey: Option[Map[String, AttributeValue]],
            acc: Source[Map[String, AttributeValue], NotUsed],
            count: Long,
            index: Int
        ): Source[Map[String, AttributeValue], NotUsed] =
          startTimeSource
            .flatMapConcat { itemStart =>
              logger.debug(s"index = $index, count = $count")
              logger.debug(s"query-batch-size = $queryBatchSize")
              val queryRequest =
                if (shardCount == 1) createNonGSIRequest(lastEvaluatedKey) else createGSIRequest(lastEvaluatedKey)
              Source
                .single(queryRequest).via(streamClient.queryFlow(1)).flatMapConcat { response =>
                  metricsReporter.setGetJournalRowsItemDuration(System.nanoTime() - itemStart)
                  if (response.sdkHttpResponse().isSuccessful) {
                    metricsReporter.incrementGetJournalRowsItemCallCounter()
                    if (response.count() > 0)
                      metricsReporter.addGetJournalRowsItemCounter(response.count().toLong)
                    val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
                    val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
                    val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                    if (lastEvaluatedKey.nonEmpty) {
                      logger.debug(s"index = $index, next loop")
                      loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
                    } else
                      combinedSource
                  } else {
                    metricsReporter.incrementGetJournalRowsItemCallErrorCounter()
                    val statusCode = response.sdkHttpResponse().statusCode()
                    val statusText = response.sdkHttpResponse().statusText()
                    logger.debug(
                      s"getJournalRows(persistenceId = $persistenceId, toSequenceNr = $toSequenceNr, deleted = $deleted): finished"
                    )
                    Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
                  }
                }
            }
        loop(None, Source.empty, 0, 1)
          .map(convertToJournalRow)
          .fold(ArrayBuffer.empty[JournalRow])(_ += _)
          .map(_.toVector)
          .map { response =>
            metricsReporter.setGetJournalRowsCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementGetJournalRowsCallCounter()
            logger.debug(
              s"getJournalRows(persistenceId = $persistenceId, toSequenceNr = $toSequenceNr, deleted = $deleted): finished"
            )
            response
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setGetJournalRowsCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementGetJournalRowsCallErrorCounter()
                logger.debug(
                  s"getJournalRows(persistenceId = $persistenceId, toSequenceNr = $toSequenceNr, deleted = $deleted): finished"
                )
                Source.failed(t)
            }
          )
          .withAttributes(logLevels)
      }
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
      .mapAsync(writeParallelism) { messages =>
        val promise = Promise[Long]()
        selectDeleteQueue(messages.head.persistenceId).offer(promise -> messages).flatMap {
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
                s"Failed to enqueue journal row batch write, the queue buffer was full ($queueBufferSize elements) please check the jdbc-journal.bufferSize setting"
              )
            )
          case QueueOfferResult.QueueClosed =>
            Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
        }
      }.withAttributes(logLevels)

  private def createNonGSIRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): QueryRequest = {
    QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpressionAsScala(
        fromSequenceNr.map(_ => "#pid = :id and #snr >= :nr").orElse(Some("#pid = :id"))
      )
      .filterExpressionAsScala(deleted.map(_ => "#d = :flg"))
      .expressionAttributeNamesAsScala(
        Map(
          "#pid" -> columnsDefConfig.partitionKeyColumnName
        ) ++ deleted.map(_ => Map("#d"     -> columnsDefConfig.deletedColumnName)).getOrElse(Map.empty) ++
        fromSequenceNr.map(_ => Map("#snr" -> columnsDefConfig.sequenceNrColumnName)).getOrElse(Map.empty)
      )
      .expressionAttributeValuesAsScala(
        Map(
          ":id" -> AttributeValue.builder().s(persistenceId.asString + "-0").build()
        ) ++ deleted
          .map(d => Map(":flg" -> AttributeValue.builder().bool(d).build())).getOrElse(Map.empty) ++ fromSequenceNr
          .map(nr => Map(":nr" -> AttributeValue.builder().n(nr.asString).build())).getOrElse(Map.empty)
      ).scanIndexForward(false)
      .limit(1).build()
  }

  private def createGSIRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): QueryRequest = {
    QueryRequest
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
  }

  private def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): Source[Long, NotUsed] = {
    startTimeSource.flatMapConcat { callStat =>
      logger.debug(
        s"highestSequenceNr(persistenceId = $persistenceId, fromSequenceNr = $fromSequenceNr, deleted = $deleted): start"
      )
      startTimeSource
        .flatMapConcat { itemStart =>
          val queryRequest =
            if (shardCount == 1) createNonGSIRequest(persistenceId, fromSequenceNr, deleted)
            else createGSIRequest(persistenceId, fromSequenceNr, deleted)
          Source
            .single(queryRequest)
            .via(streamClient.queryFlow())
            .flatMapConcat { response =>
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
                logger.debug(
                  s"highestSequenceNr(persistenceId = $persistenceId, fromSequenceNr = $fromSequenceNr, deleted = $deleted): finished"
                )
                Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
              }
            }
        }
        .map { response =>
          metricsReporter.setHighestSequenceNrCallDuration(System.nanoTime() - callStat)
          metricsReporter.incrementHighestSequenceNrCallCounter()
          logger.debug(
            s"highestSequenceNr(persistenceId = $persistenceId, fromSequenceNr = $fromSequenceNr, deleted = $deleted): finished"
          )
          response
        }.recoverWithRetries(
          attempts = 1, {
            case t: Throwable =>
              metricsReporter.setHighestSequenceNrCallDuration(System.nanoTime() - callStat)
              metricsReporter.incrementHighestSequenceNrCallErrorCounter()
              logger.debug(
                s"highestSequenceNr(persistenceId = $persistenceId, fromSequenceNr = $fromSequenceNr, deleted = $deleted): finished"
              )
              Source.failed(t)
          }
        )
        .withAttributes(logLevels)
    }
  }

  private def singleDeleteJournalRowFlow: Flow[PersistenceIdWithSeqNr, Long, NotUsed] =
    Flow[PersistenceIdWithSeqNr].flatMapConcat { persistenceIdWithSeqNr =>
      startTimeSource
        .flatMapConcat { callStart =>
          startTimeSource
            .flatMapConcat { start =>
              val deleteRequest = DeleteItemRequest
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
              Source.single(deleteRequest).via(streamClient.deleteItemFlow(1)).flatMapConcat { response =>
                metricsReporter.setDeleteJournalRowsItemDuration(System.nanoTime() - start)
                if (response.sdkHttpResponse().isSuccessful) {
                  metricsReporter.incrementDeleteJournalRowsItemCallCounter()
                  Source.single(1L)
                } else {
                  metricsReporter.incrementDeleteJournalRowsItemCallErrorCounter()
                  val statusCode = response.sdkHttpResponse().statusCode()
                  val statusText = response.sdkHttpResponse().statusText()
                  Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
                }
              }
            }.map { response =>
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
                      }.via(streamClient.batchWriteItemFlow()).flatMapConcat { response =>
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
              }
            if (persistenceIdWithSeqNrs.isEmpty)
              Source.single(0L)
            else
              SourceUtils
                .lazySource { () =>
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

  private def singlePutJournalRowFlow: Flow[JournalRow, Long, NotUsed] = Flow[JournalRow].flatMapConcat { journalRow =>
    startTimeSource
      .flatMapConcat { callStart =>
        startTimeSource
          .flatMapConcat { itemStart =>
            val request = PutItemRequest
              .builder().tableName(tableName)
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
                  .map { tag => Map(columnsDefConfig.tagsColumnName -> AttributeValue.builder().s(tag).build()) }.getOrElse(
                    Map.empty
                  )
              ).build()
            Source.single(request).via(streamClient.putItemFlow(1)).flatMapConcat { response =>
              metricsReporter.setPutJournalRowsItemDuration(System.nanoTime() - itemStart)
              if (response.sdkHttpResponse().isSuccessful) {
                metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementPutJournalRowsCallCounter()
                metricsReporter.addPutJournalRowsItemCallCounter()
                metricsReporter.incrementPutJournalRowsItemCounter()
                Source.single(1L)
              } else {
                metricsReporter.incrementPutJournalRowsCallErrorCounter()
                metricsReporter.incrementPutJournalRowsItemCallErrorCounter()
                val statusCode = response.sdkHttpResponse().statusCode()
                val statusText = response.sdkHttpResponse().statusText()
                Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
              }
            }
          }
      }
  }

  private def putJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] = Flow[Seq[JournalRow]].flatMapConcat {
    journalRows =>
      startTimeSource
        .flatMapConcat { callStart =>
          logger.debug(s"putJournalRows.size: ${journalRows.size}")
          logger.debug(s"putJournalRows: $journalRows")
          journalRows.map(p => s"pid = ${p.persistenceId}, seqNr = ${p.sequenceNumber}").foreach(logger.debug)

          def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
            Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
              startTimeSource
                .flatMapConcat { itemStart =>
                  Source
                    .single(requestItems).map { requests =>
                      BatchWriteItemRequest.builder().requestItemsAsScala(Map(tableName -> requests)).build
                    }.via(streamClient.batchWriteItemFlow()).map((_, itemStart))
                }.flatMapConcat {
                  case (response, itemStart) =>
                    metricsReporter.setPutJournalRowsItemDuration(System.nanoTime() - itemStart)
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
            SourceUtils
              .lazySource { () =>
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
                metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementPutJournalRowsCallCounter()
                response
              }
              .recoverWithRetries(
                attempts = 1, {
                  case t: Throwable =>
                    metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - callStart)
                    metricsReporter.incrementPutJournalRowsCallErrorCounter()
                    Source.failed(t)
                }
              )
        }.withAttributes(logLevels)
  }

  case class PersistenceIdWithSeqNr(persistenceId: PersistenceId, sequenceNumber: SequenceNumber)

}
