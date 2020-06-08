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
import akka.actor.{ ActorLogging, ActorSystem, DynamicAccess, ExtendedActorSystem }
import akka.event.LoggingAdapter
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1.V1JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2.V2JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils._
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

  final case class WriteFinished(pid: String, f: Future[_])

  def createV1JournalRowWriteDriver(
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      journalPluginConfig: JournalPluginConfig,
      partitionKeyResolver: PartitionKeyResolver,
      sortKeyResolver: SortKeyResolver,
      metricsReporter: Option[MetricsReporter]
  )(implicit ec: ExecutionContext, log: LoggingAdapter): V1JournalRowWriteDriver = {
    val (maybeSyncClient, maybeAsyncClient) = journalPluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val client = ClientUtils
          .createV1SyncClient(dynamicAccess, journalPluginConfig.configRootPath, journalPluginConfig)
        (Some(client), None)
      case ClientType.Async =>
        val client = ClientUtils.createV1AsyncClient(dynamicAccess, journalPluginConfig)
        (None, Some(client))
    }
    new V1JournalRowWriteDriver(
      system,
      maybeAsyncClient,
      maybeSyncClient,
      journalPluginConfig,
      partitionKeyResolver,
      sortKeyResolver,
      metricsReporter
    )
  }

  def createV2JournalRowWriteDriver(
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      journalPluginConfig: JournalPluginConfig,
      partitionKeyResolver: PartitionKeyResolver,
      sortKeyResolver: SortKeyResolver,
      metricsReporter: Option[MetricsReporter]
  )(f1: JavaDynamoDbSyncClient => Unit, f2: JavaDynamoDbAsyncClient => Unit): V2JournalRowWriteDriver = {
    val (maybeSyncClient, maybeAsyncClient) = journalPluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val client = ClientUtils.createV2SyncClient(dynamicAccess, journalPluginConfig)(f1)
        (Some(client), None)
      case ClientType.Async =>
        val client = ClientUtils.createV2AsyncClient(dynamicAccess, journalPluginConfig)(f2)
        (None, Some(client))
    }
    new V2JournalRowWriteDriver(
      system,
      maybeAsyncClient,
      maybeSyncClient,
      journalPluginConfig,
      partitionKeyResolver,
      sortKeyResolver,
      metricsReporter
    )
  }

  def createV1DaxJournalRowWriteDriver(
      system: ActorSystem,
      journalPluginConfig: JournalPluginConfig,
      partitionKeyResolver: PartitionKeyResolver,
      sortKeyResolver: SortKeyResolver,
      metricsReporter: Option[MetricsReporter]
  )(implicit ec: ExecutionContext, log: LoggingAdapter): V1JournalRowWriteDriver = {
    val (maybeSyncClient, maybeAsyncClient) = journalPluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val client =
          ClientUtils.createV1DaxSyncClient(journalPluginConfig.configRootPath, journalPluginConfig.clientConfig)
        (Some(client), None)
      case ClientType.Async =>
        val client = ClientUtils.createV1DaxAsyncClient(journalPluginConfig.clientConfig)
        (None, Some(client))
    }
    new V1JournalRowWriteDriver(
      system,
      maybeAsyncClient,
      maybeSyncClient,
      journalPluginConfig,
      partitionKeyResolver,
      sortKeyResolver,
      metricsReporter
    )
  }
}

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with ActorLogging {
  import DynamoDBJournal._

  private val id = UUID.randomUUID()

  implicit val ec: ExecutionContext   = context.dispatcher
  implicit val system: ActorSystem    = context.system
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val scheduler: Scheduler   = Scheduler(ec)
  implicit val _log                   = log

  log.debug("dynamodb journal plugin: id = {}", id)

  private val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess

  protected val journalPluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

  private val partitionKeyResolver: PartitionKeyResolver = {
    val provider = PartitionKeyResolverProvider.create(dynamicAccess, journalPluginConfig)
    provider.create
  }

  private val sortKeyResolver: SortKeyResolver = {
    val provider = SortKeyResolverProvider.create(dynamicAccess, journalPluginConfig)
    provider.create
  }

  protected val serialization: Serialization = SerializationExtension(system)

  protected val metricsReporter: Option[MetricsReporter] = {
    val metricsReporterProvider = MetricsReporterProvider.create(dynamicAccess, journalPluginConfig)
    metricsReporterProvider.create
  }

  protected val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, journalPluginConfig.tagSeparator)
  private var javaAsyncClientV2: JavaDynamoDbAsyncClient = _
  private var javaSyncClientV2: JavaDynamoDbSyncClient   = _

  private val journalRowWriteDriver: JournalRowWriteDriver = {
    journalPluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V2 =>
        createV2JournalRowWriteDriver(
          system,
          dynamicAccess,
          journalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )(v1 => javaSyncClientV2 = v1, v2 => javaAsyncClientV2 = v2)
      case ClientVersion.V1 =>
        createV1JournalRowWriteDriver(
          system,
          dynamicAccess,
          journalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )
      case ClientVersion.V1Dax =>
        createV1DaxJournalRowWriteDriver(
          system,
          journalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )
    }
  }

  protected val journalDao: JournalDaoWithUpdates =
    journalPluginConfig.journalRowDriverWrapperClassName match {
      case Some(className) =>
        val wrapper = dynamicAccess
          .createInstanceFor[JournalRowWriteDriver](
            className,
            Seq(
              classOf[JournalPluginConfig]   -> journalPluginConfig,
              classOf[JournalRowWriteDriver] -> journalRowWriteDriver
            )
          ) match {
          case Success(value) => value
          case Failure(ex)    => throw new PluginException("Failed to initialize JournalRowDriverWrapper", Some(ex))
        }
        new WriteJournalDaoImpl(
          journalPluginConfig,
          wrapper,
          serializer,
          metricsReporter
        )
      case None =>
        new WriteJournalDaoImpl(
          journalPluginConfig,
          journalRowWriteDriver,
          serializer,
          metricsReporter
        )
    }

  protected val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(atomicWrites: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
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
    future.onComplete { _ => self ! WriteFinished(persistenceId, future) }
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    journalDao
      .deleteMessages(PersistenceId(persistenceId), SequenceNumber(toSequenceNr))
      .runWith(Sink.head).map(_ => ())
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    journalDao
      .getMessagesAsPersistentReprWithBatch(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        journalPluginConfig.replayBatchSize,
        journalPluginConfig.replayBatchRefreshInterval.map((_ -> system.scheduler))
      )
      .take(max)
      .mapAsync(1)(deserializedRepr => Future.fromTry(deserializedRepr))
      .runForeach(recoveryCallback)
      .map(_ => ())
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    def fetchHighestSeqNr(): Future[Long] = {
      journalDao
        .highestSequenceNr(PersistenceId.apply(persistenceId), SequenceNumber(fromSequenceNr)).runWith(Sink.head)
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
    if (javaSyncClientV2 != null)
      javaSyncClientV2.close()
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
