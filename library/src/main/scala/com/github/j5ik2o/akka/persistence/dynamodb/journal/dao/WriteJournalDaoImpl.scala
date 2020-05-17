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

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source, SourceQueueWithComplete, SourceUtils }
import akka.stream.{ ActorMaterializer, Attributes, OverflowStrategy, QueueOfferResult }
import akka.{ Done, NotUsed }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, JournalPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.Success

case class PersistenceIdWithSeqNr(persistenceId: PersistenceId, sequenceNumber: SequenceNumber)

class WriteJournalDaoImpl(
    pluginConfig: JournalPluginConfig,
    protected val journalRowDriver: JournalRowWriteDriver,
    val serializer: FlowPersistentReprSerializer[JournalRow],
    protected val metricsReporter: MetricsReporter
)(
    implicit val ec: ExecutionContext,
    system: ActorSystem
) extends JournalDaoWithUpdates
    with DaoSupport {

  implicit val mat = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val scheduler: Scheduler = Scheduler(ec)

  private val queueBufferSize: Int  = if (pluginConfig.queueEnable) pluginConfig.queueBufferSize else 0
  private val queueParallelism: Int = if (pluginConfig.queueEnable) pluginConfig.queueParallelism else 0
  private val writeParallelism: Int = pluginConfig.writeParallelism

  private val startTimeSource: Source[Long, NotUsed] =
    SourceUtils
      .lazySource(() => Source.single(System.nanoTime())).mapMaterializedValue(_ => NotUsed)

  private val logLevels: Attributes = Attributes.logLevels(
    onElement = Attributes.LogLevels.Debug,
    onFailure = Attributes.LogLevels.Error,
    onFinish = Attributes.LogLevels.Debug
  )

  private val queueOverflowStrategy = pluginConfig.queueOverflowStrategy.toLowerCase() match {
    case s if s == OverflowStrategy.dropHead.getClass.getSimpleName.toLowerCase()     => OverflowStrategy.dropHead
    case s if s == OverflowStrategy.dropTail.getClass.getSimpleName.toLowerCase()     => OverflowStrategy.dropTail
    case s if s == OverflowStrategy.dropBuffer.getClass.getSimpleName.toLowerCase()   => OverflowStrategy.dropBuffer
    case s if s == OverflowStrategy.dropNew.getClass.getSimpleName.toLowerCase()      => OverflowStrategy.dropNew
    case s if s == OverflowStrategy.fail.getClass.getSimpleName.toLowerCase()         => OverflowStrategy.fail
    case s if s == OverflowStrategy.backpressure.getClass.getSimpleName.toLowerCase() => OverflowStrategy.backpressure
    case _                                                                            => throw new IllegalArgumentException()
  }

  private def internalPutStream(promise: Promise[Long], rows: Seq[JournalRow]): Future[Done] = {
    val s =
      if (rows.size == 1)
        Source
          .single(rows.head).via(journalRowDriver.singlePutJournalRowFlow)
      else if (rows.size > pluginConfig.clientConfig.batchWriteItemLimit)
        Source(rows.toVector)
          .grouped(pluginConfig.clientConfig.batchWriteItemLimit)
          .via(journalRowDriver.multiPutJournalRowsFlow)
          .fold(0L)(_ + _)
      else
        Source
          .single(rows)
          .via(journalRowDriver.multiPutJournalRowsFlow)
    s.map(result => promise.success(result))
      .recover { case t => promise.failure(t) }
      .runWith(Sink.ignore)
  }

  private def putQueue: SourceQueueWithComplete[(Promise[Long], Seq[JournalRow])] =
    Source
      .queue[(Promise[Long], Seq[JournalRow])](queueBufferSize, queueOverflowStrategy)
      .mapAsync(writeParallelism) {
        case (promise, rows) =>
          logger.debug(s"put rows.size = ${rows.size}")
          internalPutStream(promise, rows)
      }
      .toMat(Sink.ignore)(Keep.left)
      .withAttributes(logLevels)
      .run()

  private val putQueues = for (_ <- 1 to queueParallelism) yield putQueue

  private def queueIdFrom(persistenceId: PersistenceId): Int = Math.abs(persistenceId.asString.##) % queueParallelism

  private def selectPutQueue(persistenceId: PersistenceId): SourceQueueWithComplete[(Promise[Long], Seq[JournalRow])] =
    putQueues(queueIdFrom(persistenceId))

  private def internalDeleteStream(promise: Promise[Long], rows: Seq[PersistenceIdWithSeqNr]) = {
    val s =
      if (rows.size == 1)
        Source
          .single(rows.head).via(journalRowDriver.singleDeleteJournalRowFlow)
      else if (rows.size > pluginConfig.clientConfig.batchWriteItemLimit)
        Source(rows.toVector)
          .grouped(pluginConfig.clientConfig.batchWriteItemLimit)
          .via(journalRowDriver.multiDeleteJournalRowsFlow)
          .fold(0L)(_ + _)
      else
        Source
          .single(rows)
          .via(journalRowDriver.multiDeleteJournalRowsFlow)
    s.map(result => promise.success(result))
      .recover { case t => promise.failure(t) }
      .runWith(Sink.ignore)
  }

  private def deleteQueue: SourceQueueWithComplete[(Promise[Long], Seq[PersistenceIdWithSeqNr])] =
    Source
      .queue[(Promise[Long], Seq[PersistenceIdWithSeqNr])](queueBufferSize, queueOverflowStrategy)
      .mapAsync(writeParallelism) {
        case (promise, rows) =>
          logger.debug(s"delete rows.size = ${rows.size}")
          internalDeleteStream(promise, rows)
      }
      .toMat(Sink.ignore)(Keep.left)
      .withAttributes(logLevels)
      .run()

  private val deleteQueues = for (_ <- 1 to queueParallelism) yield deleteQueue

  private def selectDeleteQueue(
      persistenceId: PersistenceId
  ): SourceQueueWithComplete[(Promise[Long], Seq[PersistenceIdWithSeqNr])] = deleteQueues(queueIdFrom(persistenceId))

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    journalRowDriver.updateMessage(journalRow)
  }

  override def deleteMessages(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber
  ): Source[Long, NotUsed] = {
    startTimeSource
      .flatMapConcat { callStart =>
        journalRowDriver
          .getJournalRows(persistenceId, toSequenceNr, deleted = false)
          .flatMapConcat { journalRows =>
            putMessages(journalRows.map(_.withDeleted)).map(result => (result, journalRows))
          }.flatMapConcat {
            case (result, journalRows) =>
              if (!pluginConfig.softDeleted) {
                journalRowDriver
                  .highestSequenceNr(persistenceId, deleted = Some(true))
                  .flatMapConcat { highestMarkedSequenceNr =>
                    journalRowDriver
                      .getJournalRows(
                        persistenceId,
                        SequenceNumber(highestMarkedSequenceNr - 1),
                        deleted = false
                      ).flatMapConcat { _ => deleteBy(persistenceId, journalRows.map(_.sequenceNumber)) }
                  }
              } else
                Source.single(result)
          }.map { n =>
            metricsReporter.setDeleteMessagesCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementDeleteMessagesCallCounter()
            n
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setDeleteMessagesCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementDeleteMessagesCallErrorCounter()
                Source.failed(t)
            }
          )
      }.withAttributes(logLevels)
  }

  override def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed] =
    startTimeSource.flatMapConcat { callStart =>
      if (messages.isEmpty)
        Source.single(0L)
      else {
        if (pluginConfig.queueEnable)
          Source.single(messages).via(requestPutJournalRows)
        else
          Source
            .single(messages).via(requestPutJournalRowsPassThrough)
      }
    }

  override def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber
  ): Source[Long, NotUsed] = {
    journalRowDriver.highestSequenceNr(persistenceId, Some(fromSequenceNr))
  }

  private def requestPutJournalRowsPassThrough: Flow[Seq[JournalRow], Long, NotUsed] = {
    Flow[Seq[JournalRow]]
      .mapAsync(writeParallelism) { messages =>
        val promise = Promise[Long]()
        internalPutStream(promise, messages).flatMap(_ => promise.future)
      }
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

  private def deleteBy(persistenceId: PersistenceId, sequenceNrs: Seq[SequenceNumber]): Source[Long, NotUsed] = {
    if (sequenceNrs.isEmpty)
      Source.empty
    else {
      if (pluginConfig.queueEnable)
        Source
          .single(sequenceNrs.map(snr => PersistenceIdWithSeqNr(persistenceId, snr))).via(requestDeleteJournalRows)
      else
        Source
          .single(sequenceNrs.map(snr => PersistenceIdWithSeqNr(persistenceId, snr))).via(
            requestDeleteJournalRowsPassThrough
          )
    }
  }

  private def requestDeleteJournalRowsPassThrough: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] = {
    Flow[Seq[PersistenceIdWithSeqNr]]
      .mapAsync(writeParallelism) { messages =>
        val promise = Promise[Long]()
        internalDeleteStream(promise, messages).flatMap(_ => promise.future)
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

}
