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

import java.lang.management.ManagementFactory

import akka.Done
import akka.actor.{ ActorLogging, ActorSystem }
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.Sink
import akka.stream.{ ActorMaterializer, Materializer }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.jmx.{ JournalDaoStatus, MetricsFunctions }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.{ InPlaceUpdateEvent, WriteFinished }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ WriteJournalDao, WriteJournalDaoImpl }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDbClientBuilderUtils, HttpClientBuilderUtils }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.Config
import javax.management.ObjectName
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

  private case class WriteFinished(pid: String, f: Future[_])

}

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with ActorLogging {

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system
  implicit val mat: Materializer    = ActorMaterializer()
  implicit val scheduler: Scheduler = Scheduler(ec)

  private val daoStatus = new JournalDaoStatus()
  private val daoStatusName = new ObjectName(
    "com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal:type=JournalDaoStatus"
  )
  private val server = ManagementFactory.getPlatformMBeanServer
  if (!server.isRegistered(daoStatusName)) {
    server.registerMBean(daoStatus, daoStatusName)
  }

  private val metricsFunctions = MetricsFunctions(
    setPutJournalRowsDuration = { v =>
      daoStatus.setPutJournalRowsDuration(v)
    },
    addPutJournalRowsCounter = daoStatus.addPutJournalRowsCounter,
    setPutJournalRowsTotalDuration = daoStatus.setPutJournalRowsTotalDuration,
    incrementPutJournalRowsTotalCounter = () => daoStatus.incrementPutJournalRowsTotalCounter(),
    setDeleteJournalRowsDuration = daoStatus.setDeleteJournalRowsDuration,
    addDeleteJournalRowsCounter = daoStatus.addDeleteJournalRowsCounter,
    setDeleteJournalRowsTotalDuration = daoStatus.setDeleteJournalRowsTotalDuration,
    incrementDeleteJournalRowsTotalCounter = () => daoStatus.incrementDeleteJournalRowsTotalCounter(),
    setHighestSequenceNrTotalDuration = daoStatus.setHighestSequenceNrTotalDuration,
    incrementHighestSequenceNrTotalCounter = () => daoStatus.incrementHighestSequenceNrTotalCounter(),
    setMessagesDuration = daoStatus.setMessagesDuration,
    incrementMessagesCounter = () => daoStatus.incrementMessagesCounter(),
    setMessagesTotalDuration = daoStatus.setMessagesTotalDuration,
    incrementMessagesTotalCounter = () => daoStatus.incrementMessagesTotalCounter()
  )

  protected val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

  protected val httpClientBuilder: NettyNioAsyncHttpClient.Builder =
    HttpClientBuilderUtils.setup(pluginConfig.clientConfig)

  protected val dynamoDbAsyncClientBuilder: JavaDynamoDbAsyncClientBuilder =
    DynamoDbClientBuilderUtils.setup(pluginConfig.clientConfig, httpClientBuilder.build())

  protected val javaClient: JavaDynamoDbAsyncClient = dynamoDbAsyncClientBuilder.build()
  protected val asyncClient: DynamoDbAsyncClient    = DynamoDbAsyncClient(javaClient)

  protected val serialization: Serialization = SerializationExtension(system)

  protected val journalDao: WriteJournalDao =
    new WriteJournalDaoImpl(asyncClient, serialization, pluginConfig, metricsFunctions)

  protected val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, pluginConfig.tagSeparator)

  protected val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(atomicWrites: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
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
    val future =
      journalDao
        .putMessages(rowsToWrite).runWith(Sink.head).map { _ =>
          resultWhenWriteComplete.map {
            case Right(value) => Success(value)
            case Left(ex)     => Failure(ex)
          }.to
        }
    val persistenceId = atomicWrites.head.persistenceId
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

  override def postStop(): Unit = {
    javaClient.close()
    super.postStop()
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
