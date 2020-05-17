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

import java.util.UUID

import akka.Done
import akka.actor.{ ActorLogging, ActorSystem, ExtendedActorSystem }
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ ClientType, ClientVersion, JournalPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.{ InPlaceUpdateEvent, WriteFinished }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao._
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDbAsyncClient, DynamoDbSyncClient }
import com.typesafe.config.Config
import monix.execution.Scheduler
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import scala.collection.immutable._
import scala.collection.{ immutable, mutable }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object DynamoDBJournal {

  final case class InPlaceUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef)

  case class WriteFinished(pid: String, f: Future[_])

}

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with ActorLogging {

  private val id = UUID.randomUUID()

  implicit val ec: ExecutionContext   = context.dispatcher
  implicit val system: ActorSystem    = context.system
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val scheduler: Scheduler   = Scheduler(ec)

  log.debug("dynamodb journal plugin: id = {}", id)

  private val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess

  protected val jounalPluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

  private val partitionKeyResolver: PartitionKeyResolver = {
    val className = jounalPluginConfig.partitionKeyResolverClassName
    val args =
      Seq(classOf[Config] -> config)
    dynamicAccess
      .createInstanceFor[PartitionKeyResolver](
        className,
        args
      ).getOrElse(throw new ClassNotFoundException(className))
  }

  private val sortKeyResolver: SortKeyResolver = {
    val className = jounalPluginConfig.sortKeyResolverClassName
    val args =
      Seq(classOf[Config] -> config)
    dynamicAccess
      .createInstanceFor[SortKeyResolver](
        className,
        args
      ).getOrElse(throw new ClassNotFoundException(className))
  }

  protected val serialization: Serialization = SerializationExtension(system)

  protected val metricsReporter: MetricsReporter = MetricsReporter.create(jounalPluginConfig.metricsReporterClassName)

  protected val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, jounalPluginConfig.tagSeparator)
  private var javaAsyncClientV2: JavaDynamoDbAsyncClient = _
  private var javaSyncClientV2: JavaDynamoDbSyncClient   = _

  private val journalRowWriteDriver: JournalRowWriteDriver = {
    jounalPluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V2 =>
        val clientOverrideConfiguration = V2ClientOverrideConfigurationUtils.setup(jounalPluginConfig.clientConfig)
        val (maybeSyncClient, maybeAsyncClient) = jounalPluginConfig.clientConfig.clientType match {
          case ClientType.Sync =>
            val httpSyncClient =
              V2HttpClientBuilderUtils.setupSync(jounalPluginConfig.clientConfig)
            javaSyncClientV2 = V2DynamoDbClientBuilderUtils.setupSync(
              jounalPluginConfig.clientConfig,
              httpSyncClient,
              clientOverrideConfiguration
            )
            val syncClient: DynamoDbSyncClient = DynamoDbSyncClient(javaSyncClientV2)
            (Some(syncClient), None)
          case ClientType.Async =>
            val httpAsyncClient =
              V2HttpClientBuilderUtils.setupAsync(jounalPluginConfig.clientConfig)
            javaAsyncClientV2 = V2DynamoDbClientBuilderUtils.setupAsync(
              jounalPluginConfig.clientConfig,
              httpAsyncClient,
              clientOverrideConfiguration
            )
            val asyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(javaAsyncClientV2)
            (None, Some(asyncClient))
        }
        new V2JournalRowWriteDriver(
          maybeAsyncClient,
          maybeSyncClient,
          jounalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )
      case ClientVersion.V1 =>
        val (maybeSyncClient, maybeAsyncClient) = jounalPluginConfig.clientConfig.clientType match {
          case ClientType.Sync =>
            (Some(V1DynamoDBClientBuilderUtils.setupSync(dynamicAccess, jounalPluginConfig.clientConfig)), None)
          case ClientType.Async =>
            (None, Some(V1DynamoDBClientBuilderUtils.setupAsync(dynamicAccess, jounalPluginConfig.clientConfig)))
        }
        new V1JournalRowWriteDriver(
          maybeAsyncClient,
          maybeSyncClient,
          jounalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )
      case ClientVersion.V1Dax =>
        val (maybeSyncClient, maybeAsyncClient) = jounalPluginConfig.clientConfig.clientType match {
          case ClientType.Sync =>
            (Some(V1DaxClientBuilderUtils.setupSync(jounalPluginConfig)), None)
          case ClientType.Async =>
            (None, Some(V1DaxClientBuilderUtils.setupAsync(jounalPluginConfig)))
        }
        new V1JournalRowWriteDriver(
          maybeAsyncClient,
          maybeSyncClient,
          jounalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )
    }
  }

  protected val journalDao: JournalDaoWithUpdates =
    new WriteJournalDaoImpl(
      jounalPluginConfig,
      journalRowWriteDriver,
      serializer,
      metricsReporter
    )

  protected val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(atomicWrites: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val startTime                                                = System.nanoTime()
    val serializedTries: Seq[Either[Throwable, Seq[JournalRow]]] = serializer.serialize(atomicWrites)
    val rowsToWrite: Seq[JournalRow] = for {
      serializeTry <- serializedTries
      row <- serializeTry match {
        case Right(value) => value
        case Left(_)      => Seq.empty
      }
    } yield row

    def resultWhenWriteComplete: Seq[Either[Throwable, Unit]] =
      if (serializedTries.forall(_.isRight)) Nil
      else
        serializedTries.map {
          case Right(_) => Right(())
          case Left(ex) => Left(ex)
        }

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
    }
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
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
    }
    future
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    val startTime = System.nanoTime()
    val future = journalDao
      .getMessagesAsPersistentReprWithBatch(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        jounalPluginConfig.replayBatchSize,
        jounalPluginConfig.replayBatchRefreshInterval.map((_ -> system.scheduler))
      )
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
    }
    future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    def fetchHighestSeqNr(): Future[Long] = {
      val startTime = System.nanoTime()
      val result =
        journalDao
          .highestSequenceNr(PersistenceId.apply(persistenceId), SequenceNumber(fromSequenceNr)).runWith(Sink.head)
      result.onComplete { result =>
        metricsReporter.setAsyncReadHighestSequenceNrCallDuration(System.nanoTime() - startTime)
        if (result.isSuccess)
          metricsReporter.incrementAsyncReadHighestSequenceNrCallCounter()
        else
          metricsReporter.incrementAsyncReadHighestSequenceNrCallErrorCounter()
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
    if (javaAsyncClientV2 != null)
      javaAsyncClientV2.close()
    writeInProgress.clear()
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
