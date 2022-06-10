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
package com.github.j5ik2o.akka.persistence.dynamodb.snapshot

import akka.actor.ExtendedActorSystem
import akka.event.LoggingAdapter
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.{ Materializer, SystemMaterializer }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.{ SnapshotDao, V1SnapshotDaoImpl, V2SnapshotDaoImpl }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ClientUtils
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

object DynamoDBSnapshotStore {

  def toSelectedSnapshot(tupled: (SnapshotMetadata, Any)): SelectedSnapshot = tupled match {
    case (meta: SnapshotMetadata, snapshot: Any) => SelectedSnapshot(meta, snapshot)
  }

}

class DynamoDBSnapshotStore(config: Config) extends SnapshotStore {
  import DynamoDBSnapshotStore._

  implicit val ec: ExecutionContext        = context.dispatcher
  implicit val system: ExtendedActorSystem = context.system.asInstanceOf[ExtendedActorSystem]
  implicit val mat: Materializer           = SystemMaterializer(system).materializer
  implicit val _log: LoggingAdapter        = log

  private val dynamicAccess = system.dynamicAccess

  private val serialization                        = SerializationExtension(system)
  protected val pluginConfig: SnapshotPluginConfig = SnapshotPluginConfig.fromConfig(config)

  protected var v2JavaAsyncClient: JavaDynamoDbAsyncClient = _
  protected var v2JavaSyncClient: JavaDynamoDbSyncClient   = _

  protected var v1JavaAsyncClient: AmazonDynamoDBAsync = _
  protected var v1JavaSyncClient: AmazonDynamoDB       = _

  protected val metricsReporter: Option[MetricsReporter] = {
    val metricsReporterProvider = MetricsReporterProvider.create(dynamicAccess, pluginConfig)
    metricsReporterProvider.create
  }

  protected val traceReporter: Option[TraceReporter] = {
    val traceReporterProvider = TraceReporterProvider.create(dynamicAccess, pluginConfig)
    traceReporterProvider.create
  }

  override def postStop(): Unit = {
    if (v2JavaAsyncClient != null)
      v2JavaAsyncClient.close()
    if (v2JavaSyncClient != null)
      v2JavaSyncClient.close()
    super.postStop()
  }

  protected val snapshotDao: SnapshotDao = {
    pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V2 =>
        pluginConfig.clientConfig.clientType match {
          case ClientType.Async =>
            val client =
              ClientUtils.createV2AsyncClient(system.dynamicAccess, pluginConfig)(c => v2JavaAsyncClient = c)
            new V2SnapshotDaoImpl(
              system,
              Some(client),
              None,
              serialization,
              pluginConfig,
              metricsReporter,
              traceReporter
            )
          case ClientType.Sync =>
            val client =
              ClientUtils.createV2SyncClient(system.dynamicAccess, pluginConfig.configRootPath, pluginConfig)(c =>
                v2JavaSyncClient = c
              )
            new V2SnapshotDaoImpl(
              system,
              None,
              Some(client),
              serialization,
              pluginConfig,
              metricsReporter,
              traceReporter
            )
        }
      case ClientVersion.V1 =>
        pluginConfig.clientConfig.clientType match {
          case ClientType.Async =>
            v1JavaAsyncClient = ClientUtils.createV1AsyncClient(system.dynamicAccess, pluginConfig)
            new V1SnapshotDaoImpl(
              system,
              Some(v1JavaAsyncClient),
              None,
              serialization,
              pluginConfig,
              metricsReporter,
              traceReporter
            )
          case ClientType.Sync =>
            v1JavaSyncClient =
              ClientUtils.createV1SyncClient(system.dynamicAccess, pluginConfig.configRootPath, pluginConfig)
            new V1SnapshotDaoImpl(
              system,
              None,
              Some(v1JavaSyncClient),
              serialization,
              pluginConfig,
              metricsReporter,
              traceReporter
            )
        }
      case ClientVersion.V1Dax =>
        pluginConfig.clientConfig.clientType match {
          case ClientType.Async =>
            v1JavaAsyncClient = ClientUtils.createV1DaxAsyncClient(pluginConfig.clientConfig)
            new V1SnapshotDaoImpl(
              system,
              Some(v1JavaAsyncClient),
              None,
              serialization,
              pluginConfig,
              metricsReporter,
              traceReporter
            )
          case ClientType.Sync =>
            v1JavaSyncClient = ClientUtils.createV1DaxSyncClient(pluginConfig.configRootPath, pluginConfig.clientConfig)
            new V1SnapshotDaoImpl(
              system,
              None,
              Some(v1JavaSyncClient),
              serialization,
              pluginConfig,
              metricsReporter,
              traceReporter
            )
        }

    }
  }

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreLoadAsync(context))

    def future = {
      val result = criteria match {
        case SnapshotSelectionCriteria(Long.MaxValue, Long.MaxValue, _, _) =>
          snapshotDao.latestSnapshot(PersistenceId(persistenceId))
        case SnapshotSelectionCriteria(Long.MaxValue, maxTimestamp, _, _) =>
          snapshotDao.snapshotForMaxTimestamp(PersistenceId(persistenceId), maxTimestamp)
        case SnapshotSelectionCriteria(maxSequenceNr, Long.MaxValue, _, _) =>
          snapshotDao.snapshotForMaxSequenceNr(PersistenceId(persistenceId), SequenceNumber(maxSequenceNr))
        case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, _, _) =>
          snapshotDao.snapshotForMaxSequenceNrAndMaxTimestamp(
            PersistenceId(persistenceId),
            SequenceNumber(maxSequenceNr),
            maxTimestamp
          )
        case _ => Source.empty
      }
      result.map(_.map(toSelectedSnapshot)).runWith(Sink.head)
    }

    val traced = traceReporter.fold(future)(_.traceSnapshotStoreLoadAsync(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterSnapshotStoreLoadAsync(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorSnapshotStoreLoadAsync(newContext, ex))
    }
    traced
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val pid        = PersistenceId(metadata.persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreSaveAsync(context))

    def future = snapshotDao.save(metadata, snapshot).runWith(Sink.ignore).map(_ => ())

    val traced = traceReporter.fold(future)(_.traceSnapshotStoreSaveAsync(context)(future))
    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterSnapshotStoreSaveAsync(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorSnapshotStoreSaveAsync(newContext, ex))
    }
    traced
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val pid        = PersistenceId(metadata.persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreDeleteAsync(context))

    def future = snapshotDao
      .delete(PersistenceId(metadata.persistenceId), SequenceNumber(metadata.sequenceNr)).map(_ => ()).runWith(
        Sink.ignore
      ).map(_ => ())

    val traced = traceReporter.fold(future)(_.traceSnapshotStoreDeleteAsync(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterSnapshotStoreDeleteAsync(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorSnapshotStoreDeleteAsync(newContext, ex))
    }
    traced
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreDeleteWithCriteriaAsync(context))

    def future = criteria match {
      case SnapshotSelectionCriteria(Long.MaxValue, Long.MaxValue, _, _) =>
        snapshotDao.deleteAllSnapshots(pid).runWith(Sink.ignore).map(_ => ())
      case SnapshotSelectionCriteria(Long.MaxValue, maxTimestamp, _, _) =>
        snapshotDao.deleteUpToMaxTimestamp(pid, maxTimestamp).runWith(Sink.ignore).map(_ => ())
      case SnapshotSelectionCriteria(maxSequenceNr, Long.MaxValue, _, _) =>
        snapshotDao
          .deleteUpToMaxSequenceNr(pid, SequenceNumber(maxSequenceNr)).runWith(Sink.ignore).map(_ => ())
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, _, _) =>
        snapshotDao
          .deleteUpToMaxSequenceNrAndMaxTimestamp(pid, SequenceNumber(maxSequenceNr), maxTimestamp).runWith(
            Sink.ignore
          ).map(_ => ())
      case _ => Future.successful(())
    }

    val traced = traceReporter.fold(future)(_.traceSnapshotStoreDeleteWithCriteriaAsync(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterSnapshotStoreDeleteWithCriteriaAsync(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorSnapshotStoreDeleteWithCriteriaAsync(newContext, ex))
    }
    traced
  }

}
