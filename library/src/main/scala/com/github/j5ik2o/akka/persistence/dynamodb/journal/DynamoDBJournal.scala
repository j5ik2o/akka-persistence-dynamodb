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
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.{ InPlaceUpdateEvent, WriteFinished }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ JournalDaoWithUpdates, WriteJournalDaoImpl }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDbClientBuilderUtils, HttpClientBuilderUtils }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.Config
import monix.execution.Scheduler
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbAsyncClientBuilder => JavaDynamoDbAsyncClientBuilder
}

import scala.collection.{ immutable, mutable }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object DynamoDBJournal {

  final case class InPlaceUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef)

  case class WriteFinished(pid: String, f: Future[_])

}

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with ActorLogging {

  implicit val ec: ExecutionContext   = context.dispatcher
  implicit val system: ActorSystem    = context.system
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val scheduler: Scheduler   = Scheduler(ec)

  protected val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

  protected val httpClientBuilder: NettyNioAsyncHttpClient.Builder =
    HttpClientBuilderUtils.setup(pluginConfig.clientConfig)

  protected val dynamoDbAsyncClientBuilder: JavaDynamoDbAsyncClientBuilder =
    DynamoDbClientBuilderUtils.setup(pluginConfig.clientConfig, httpClientBuilder.build())

  protected val javaClient: JavaDynamoDbAsyncClient = dynamoDbAsyncClientBuilder.build()
  protected val asyncClient: DynamoDbAsyncClient    = DynamoDbAsyncClient(javaClient)

  protected val serialization: Serialization = SerializationExtension(system)

  protected val metricsReporter: MetricsReporter = MetricsReporter.create(pluginConfig.metricsReporterClassName)

  protected val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, pluginConfig.tagSeparator)

  protected val journalDao: JournalDaoWithUpdates =
    new WriteJournalDaoImpl(asyncClient, serialization, pluginConfig, serializer, metricsReporter)

  protected val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(atomicWrites: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    log.debug(s"asyncWriteMessages($atomicWrites): start")
    val startTime = System.nanoTime()

    val serializedTries: Seq[Either[Throwable, Seq[JournalRow]]] = serializer.serialize(atomicWrites)
    val rowsToWrite: Seq[JournalRow] = for {
      serializeTry <- serializedTries
      row          <- serializeTry.right.getOrElse(Seq.empty)
    } yield row

    def resultWhenWriteComplete: Seq[Either[Throwable, Unit]] =
      if (serializedTries.forall(_.isRight)) Nil
      else
        serializedTries
          .map(_.right.map(_ => ()))

    val future: Future[immutable.Seq[Try[Unit]]] =
      journalDao
        .putMessages(rowsToWrite).runWith(Sink.head).recoverWith {
          case ex =>
            log.error(ex, "occurred error")
            Future.failed(ex)
        }.map { _ =>
          resultWhenWriteComplete.map {
            case Right(value) => Success(value)
            case Left(ex)     => Failure(ex)
          }.toVector
        }
    val persistenceId = atomicWrites.head.persistenceId
    writeInProgress.put(persistenceId, future)

    future.onComplete { result =>
      self ! WriteFinished(persistenceId, future)
      metricsReporter.setAsyncWriteMessagesCallDuration(System.nanoTime() - startTime)
      if (result.isSuccess)
        metricsReporter.incrementAsyncWriteMessagesCallCounter()
      else
        metricsReporter.incrementAsyncWriteMessagesCallErrorCounter()
      log.debug(s"asyncWriteMessages($atomicWrites): finished")
    }
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug(s"asyncDeleteMessagesTo($persistenceId, $toSequenceNr): start")
    val startTime = System.nanoTime()
    val future = journalDao
      .deleteMessages(PersistenceId(persistenceId), SequenceNumber(toSequenceNr))
      .runWith(Sink.head).map(_ => ())
    future.onComplete { result =>
      metricsReporter.setAsyncDeleteMessagesToCallDuration(System.nanoTime() - startTime)
      if (result.isSuccess)
        metricsReporter.incrementAsyncDeleteMessagesToCallCounter()
      else
        metricsReporter.incrementAsyncDeleteMessagesToCallErrorCounter()
      log.debug(s"asyncDeleteMessagesTo($persistenceId, $toSequenceNr): finished")
    }
    future
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    log.debug(s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): start")
    val startTime = System.nanoTime()
    val future = journalDao
      .getMessagesWithBatch(persistenceId, fromSequenceNr, toSequenceNr, pluginConfig.replayBatchSize, None)
      .take(max)
      .mapAsync(1)(deserializedRepr => Future.fromTry(deserializedRepr))
      .runForeach(recoveryCallback)
      .map(_ => ())
    future.onComplete { result =>
      metricsReporter.setAsyncReplayMessagesCallDuration(System.nanoTime() - startTime)
      if (result.isSuccess)
        metricsReporter.incrementAsyncReplayMessagesCallCounter()
      else
        metricsReporter.incrementAsyncReplayMessagesCallErrorCounter()
      log.debug(s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): finished")
    }
    future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug(s"asyncReadHighestSequenceNr($persistenceId, $fromSequenceNr): start")
    def fetchHighestSeqNr(): Future[Long] = {
      val startTime = System.nanoTime()
      val result =
        journalDao.highestSequenceNr(PersistenceId(persistenceId), SequenceNumber(fromSequenceNr)).runWith(Sink.head)
      result.onComplete { result =>
        metricsReporter.setAsyncReadHighestSequenceNrCallDuration(System.nanoTime() - startTime)
        if (result.isSuccess)
          metricsReporter.incrementAsyncReadHighestSequenceNrCallCounter()
        else
          metricsReporter.incrementAsyncReadHighestSequenceNrCallErrorCounter()
        log.debug(s"asyncReadHighestSequenceNr($persistenceId, $fromSequenceNr): finished")
      }
      result
    }

    val future = writeInProgress.get(persistenceId) match {
      case None    => fetchHighestSeqNr()
      case Some(f) =>
        // we must fetch the highest sequence number after the previous write has completed
        // If the previous write failed then we can ignore this
        f.recover { case _ => () }.flatMap(_ => fetchHighestSeqNr())
    }

    future
  }

  override def postStop(): Unit = {
    javaClient.close()
    writeInProgress.clear()
    super.postStop()
  }

  override def receivePluginInternal: Receive = {
    case msg @ WriteFinished(persistenceId, _) =>
      log.debug(s"receivePluginInternal:$msg: start")
      writeInProgress.remove(persistenceId)
      log.debug(s"receivePluginInternal:$msg: finished")
    case msg @ InPlaceUpdateEvent(pid, seq, message) =>
      log.debug(s"receivePluginInternal:$msg: start")
      asyncUpdateEvent(pid, seq, message).pipeTo(sender())
      log.debug(s"receivePluginInternal:$msg: finished")
  }

  private def asyncUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef): Future[Done] = {
    log.debug(s"asyncUpdateEvent($persistenceId, $sequenceNumber, $message): start")
    val write = PersistentRepr(message, sequenceNumber, persistenceId)
    val serializedRow: JournalRow = serializer.serialize(write) match {
      case Right(row) => row
      case Left(_) =>
        throw new IllegalArgumentException(
          s"Failed to serialize ${write.getClass} for update of [$persistenceId] @ [$sequenceNumber]"
        )
    }
    val future = journalDao.updateMessage(serializedRow).runWith(Sink.ignore)
    future.onComplete { _ => log.debug(s"asyncUpdateEvent($persistenceId, $sequenceNumber, $message): finished") }
    future
  }
}
