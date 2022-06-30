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
package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.Done
import akka.actor.{ ActorLogging, ActorSystem }
import akka.event.LoggingAdapter
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.Sink
import akka.stream.{ Materializer, SystemMaterializer }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId, SequenceNumber }
import com.typesafe.config.Config

import java.util.UUID
import scala.collection.immutable._
import scala.collection.{ immutable, mutable }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object DynamoDBJournal {

  final case class InPlaceUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef)

  final case class WriteFinished(pid: String, f: Future[_])

}

final class DynamoDBJournal(config: Config) extends AsyncWriteJournal with ActorLogging {

  import DynamoDBJournal._

  private val id = UUID.randomUUID()

  implicit val system: ActorSystem  = context.system
  implicit val mat: Materializer    = SystemMaterializer(system).materializer
  implicit val _log: LoggingAdapter = log

  log.debug("dynamodb journal plugin: id = {}", id)

  protected val serialization: Serialization = SerializationExtension(system)

  protected val journalPluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)
  private val pluginContext                              = JournalPluginContext(system, journalPluginConfig)

  import pluginContext._

  protected val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, journalPluginConfig.tagSeparator, metricsReporter, traceReporter)

  protected val journalDao: JournalDaoWithUpdates =
    journalPluginConfig.journalRowDriverWrapperClassName match {
      case Some(className) =>
        val wrapper = pluginContext.newDynamicAccessor[JournalRowWriteDriver]().createThrow(className)
        new WriteJournalDaoImpl(
          pluginContext,
          wrapper,
          serializer
        )(pluginExecutor, system)
      case None =>
        new WriteJournalDaoImpl(
          pluginContext,
          journalRowWriteDriver,
          serializer
        )(pluginExecutor, system)
    }

  protected val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(atomicWrites: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val persistenceId = atomicWrites.head.persistenceId
    val pid           = PersistenceId(persistenceId)
    val context       = Context.newContext(UUID.randomUUID(), pid)
    val newContext    = metricsReporter.fold(context)(_.beforeJournalAsyncWriteMessages(context))

    implicit val ec: ExecutionContext = pluginExecutor

    def execute(implicit ec: ExecutionContext): Future[Vector[Try[Unit]]] = {
      val serializedFutures = serializer.serialize(atomicWrites)
      val rowsToWriteFutures = serializedFutures.map { serializeFuture =>
        serializeFuture.recoverWith { case _ =>
          Future.successful(Seq.empty)
        }
      }
      val resultWhenWriteComplete: Future[Vector[Future[Unit]]] = {
        Future.sequence(serializedFutures).map(_ => true).recover { case _ => false }.map { b =>
          if (b) {
            Vector.empty
          } else {
            serializedFutures.toVector.map(s => s.map(_ => ()))
          }
        }
      }

      Future
        .traverse(rowsToWriteFutures) { rowsToWriteFuture =>
          rowsToWriteFuture.flatMap { rowsToWrite =>
            journalDao
              .putMessages(rowsToWrite).runWith(Sink.head).recoverWith { case ex =>
                log.error(ex, "occurred error")
                Future.failed(ex)
              }
          }
        }.flatMap { _ =>
          resultWhenWriteComplete.flatMap { future =>
            future.foldLeft(Future.successful(Vector.empty[Try[Unit]])) { (result, element) =>
              (for {
                r <- result
                e <- element
              } yield r :+ Success(e))
                .recoverWith { case ex =>
                  result.map(_ :+ Failure(ex))
                }
            }
          }
        }
    }

    val future = traceReporter.fold(execute)(_.traceJournalAsyncWriteMessages(newContext)(execute))

    writeInProgress.put(persistenceId, future)
    future.onComplete { result: Try[Seq[Try[Unit]]] =>
      self ! WriteFinished(persistenceId, future)
      result match {
        case Success(_) =>
          metricsReporter.foreach(_.afterJournalAsyncWriteMessages(newContext))
        case Failure(ex) =>
          metricsReporter.foreach(_.errorJournalAsyncWriteMessages(newContext, ex))
      }
    }
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeJournalAsyncDeleteMessagesTo(context))

    implicit val ec: ExecutionContext = pluginExecutor

    def future = journalDao
      .deleteMessages(PersistenceId(persistenceId), SequenceNumber(toSequenceNr))
      .runWith(Sink.head).map(_ => ())

    val traced = traceReporter.fold(future)(_.traceJournalAsyncDeleteMessagesTo(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterJournalAsyncDeleteMessagesTo(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorJournalAsyncDeleteMessagesTo(newContext, ex))
    }
    traced
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    implicit val ec: ExecutionContext = pluginExecutor

    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeJournalAsyncReplayMessages(context))

    def future = journalDao
      .getMessagesAsPersistentReprWithBatch(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        journalPluginConfig.replayBatchSize,
        journalPluginConfig.replayBatchRefreshInterval.map(_ -> system.scheduler)
      )
      .take(max)
      .mapAsync(1)(deserializedRepr => Future.fromTry(deserializedRepr))
      .runForeach(recoveryCallback)
      .map(_ => ())

    val traced = traceReporter.fold(future)(_.traceJournalAsyncReplayMessages(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterJournalAsyncReplayMessages(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorJournalAsyncReplayMessages(newContext, ex))
    }
    traced
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    implicit val ec: ExecutionContext = pluginExecutor
    val pid                           = PersistenceId(persistenceId)
    val context                       = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeJournalAsyncReadHighestSequenceNr(context))

    def fetchHighestSeqNr(): Future[Long] = {
      journalDao
        .highestSequenceNr(PersistenceId.apply(persistenceId), SequenceNumber(fromSequenceNr)).map(
          _.getOrElse(0L)
        ).runWith(Sink.head)
    }

    def future = writeInProgress.get(persistenceId) match {
      case None    => fetchHighestSeqNr()
      case Some(f) =>
        // we must fetch the highest sequence number after the previous write has completed
        // If the previous write failed then we can ignore this
        f.recover { case _ => () }.flatMap(_ => fetchHighestSeqNr())
    }

    val traced = traceReporter.fold(future)(_.traceJournalAsyncReadHighestSequenceNr(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterJournalAsyncReadHighestSequenceNr(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorJournalAsyncReadHighestSequenceNr(newContext, ex))
    }

    traced
  }

  override def postStop(): Unit = {
    journalDao.dispose()
    journalRowWriteDriver.dispose()
    writeInProgress.clear()
    super.postStop()
  }

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, _) =>
      writeInProgress.remove(persistenceId)
    case InPlaceUpdateEvent(pid, seq, message) =>
      implicit val ec: ExecutionContext = pluginExecutor
      asyncUpdateEvent(pid, seq, message).pipeTo(sender())
  }

  private def asyncUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef): Future[Done] = {
    implicit val ec: ExecutionContext = pluginExecutor

    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeJournalAsyncUpdateEvent(context))

    def future = {
      val write = PersistentRepr(message, sequenceNumber, persistenceId)
      serializer
        .serialize(write).flatMap { serializedRow =>
          journalDao.updateMessage(serializedRow).runWith(Sink.ignore)
        }.recoverWith { case _ =>
          Future.failed(
            new IllegalArgumentException(
              s"Failed to serialize ${write.getClass} for update of [$persistenceId] @ [$sequenceNumber]"
            )
          )
        }
    }

    val traced = traceReporter.fold(future)(_.traceJournalAsyncUpdateEvent(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterJournalAsyncUpdateEvent(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorJournalAsyncUpdateEvent(newContext, ex))
    }

    traced
  }
}
