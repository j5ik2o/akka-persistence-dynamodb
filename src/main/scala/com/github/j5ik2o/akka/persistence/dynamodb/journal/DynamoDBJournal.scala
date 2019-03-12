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
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Sink
import akka.stream.{ ActorMaterializer, Materializer }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.{ InPlaceUpdateEvent, WriteFinished }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDbClientBuilderUtils, HttpClientUtils }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import com.typesafe.config.Config
import monix.execution.Scheduler
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.collection.{ immutable, mutable }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object DynamoDBJournal {

  final case class InPlaceUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef)

  private case class WriteFinished(pid: String, f: Future[_])

}

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with ActorLogging {
  import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{
    WriteJournalDao,
    WriteJournalDaoImpl,
    WriteJournalDaoWithUpdates
  }

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system
  implicit val mat: Materializer    = ActorMaterializer()
  implicit val scheduler: Scheduler = Scheduler(ec)

  protected val pluginConfig: JournalPluginConfig =
    JournalPluginConfig.fromConfig(config)

  protected val tableName: String = pluginConfig.tableName

  private val httpClientBuilder = HttpClientUtils.asyncBuilder(pluginConfig)

  private val dynamoDbAsyncClientBuilder =
    DynamoDbClientBuilderUtils.asyncBuilder(pluginConfig, httpClientBuilder.build())

  protected val javaClient: DynamoDbAsyncClient    = dynamoDbAsyncClientBuilder.build()
  protected val asyncClient: DynamoDBAsyncClientV2 = DynamoDBAsyncClientV2(javaClient)

  private val serialization = SerializationExtension(system)

  protected val journalDao: WriteJournalDao with WriteJournalDaoWithUpdates =
    new WriteJournalDaoImpl(asyncClient, serialization, pluginConfig)

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, pluginConfig.tagSeparator)

  private val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val serializedTries: Seq[Either[Throwable, Seq[JournalRow]]] = serializer.serialize(messages)
    val rowsToWrite: Seq[JournalRow] = for {
      serializeTry <- serializedTries
      row          <- serializeTry.right.getOrElse(Seq.empty)
    } yield row
    def resultWhenWriteComplete: Seq[Either[Throwable, Unit]] =
      if (serializedTries.forall(_.isRight)) Nil
      else
        serializedTries
          .map(_.right.map(_ => ()))
    val future =
      journalDao
        .putMessages(rowsToWrite).runWith(Sink.head).map(
          _ =>
            resultWhenWriteComplete.map {
              case Right(value) => Success(value)
              case Left(ex)     => Failure(ex)
            }.to
        )
    val persistenceId = messages.head.persistenceId
    writeInProgress.put(persistenceId, future)
    future.onComplete(_ => self ! WriteFinished(persistenceId, future))
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    journalDao
      .deleteMessages(PersistenceId(persistenceId), SequenceNumber(toSequenceNr)).runWith(Sink.head).map(_ => ())
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] =
    journalDao
      .getMessages(PersistenceId(persistenceId), SequenceNumber(fromSequenceNr), SequenceNumber(toSequenceNr), max)
      .via(serializer.deserializeFlowWithoutTags)
      .runForeach(recoveryCallback)
      .map(_ => ())

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    def fetchHighestSeqNr(): Future[Long] =
      journalDao.highestSequenceNr(PersistenceId(persistenceId), SequenceNumber(fromSequenceNr)).runWith(Sink.head)

    writeInProgress.get(persistenceId) match {
      case None    => fetchHighestSeqNr()
      case Some(f) =>
        // we must fetch the highest sequence number after the previous write has completed
        // If the previous write failed then we can ignore this
        f.recover { case _ => () }.flatMap(_ => fetchHighestSeqNr())
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    log.debug("start")
  }

  override def postStop(): Unit = {
    javaClient.close()
    super.postStop()
    log.debug("stop")
  }

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, _) =>
      writeInProgress.remove(persistenceId)
    case InPlaceUpdateEvent(pid, seq, message) =>
      asyncUpdateEvent(pid, seq, message).pipeTo(sender())
  }

  private def asyncUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef): Future[Done] = {
    val write = PersistentRepr(message, sequenceNumber, persistenceId)
    val serializedRow: JournalRow = serializer.serialize(write) match {
      case Right(row) => row
      case Left(_) =>
        throw new IllegalArgumentException(
          s"Failed to serialize ${write.getClass} for update of [$persistenceId] @ [$sequenceNumber]"
        )
    }
    journalDao.updateMessage(serializedRow).runWith(Sink.ignore)
  }
}
