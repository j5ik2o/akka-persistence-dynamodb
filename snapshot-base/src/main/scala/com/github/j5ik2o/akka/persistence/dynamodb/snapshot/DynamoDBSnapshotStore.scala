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
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.{ SnapshotDao, SnapshotDaoFactory }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.typesafe.config.Config

import java.util.UUID
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

object DynamoDBSnapshotStore {

  def toSelectedSnapshot(tupled: (SnapshotMetadata, Any)): SelectedSnapshot = tupled match {
    case (meta: SnapshotMetadata, snapshot: Any) => SelectedSnapshot(meta, snapshot)
  }

}

final class DynamoDBSnapshotStore(config: Config) extends SnapshotStore {

  import DynamoDBSnapshotStore._

  implicit val system: ExtendedActorSystem = context.system.asInstanceOf[ExtendedActorSystem]
  implicit val mat: Materializer           = SystemMaterializer(system).materializer
  implicit val _log: LoggingAdapter        = log

  private val dynamicAccess = system.dynamicAccess

  private val serialization                        = SerializationExtension(system)
  protected val pluginConfig: SnapshotPluginConfig = SnapshotPluginConfig.fromConfig(config)

  private val pluginExecutor: ExecutionContext =
    pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V1 =>
        DispatcherUtils.newV1Executor(pluginConfig, system)
      case ClientVersion.V2 =>
        DispatcherUtils.newV2Executor(pluginConfig, system)
    }

  private implicit val ec: ExecutionContext = pluginExecutor

  private val metricsReporter: Option[MetricsReporter] = {
    val metricsReporterProvider = MetricsReporterProvider.create(dynamicAccess, pluginConfig)
    metricsReporterProvider.create
  }

  private val traceReporter: Option[TraceReporter] = {
    val traceReporterProvider = TraceReporterProvider.create(dynamicAccess, pluginConfig)
    traceReporterProvider.create
  }

  private val snapshotDao: SnapshotDao = {
    val className = pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V2 =>
        "com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.V2SnapshotDaoFactory"
      case ClientVersion.V1 =>
        "com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.V1SnapshotDaoFactory"
      case ClientVersion.V1Dax =>
        "com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.V1DaxSnapshotDaoFactory"
    }
    val f = dynamicAccess.createInstanceFor[SnapshotDaoFactory](className, immutable.Seq.empty).get
    f.create(
      system,
      dynamicAccess,
      serialization,
      pluginConfig,
      metricsReporter,
      traceReporter
    )
  }

  override def postStop(): Unit = {
    snapshotDao.dispose()
    super.postStop()
  }

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeSnapshotStoreLoadAsync(context))

    def future: Future[Option[SelectedSnapshot]] = {
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

    def future: Future[Unit] = snapshotDao.save(metadata, snapshot).runWith(Sink.ignore).map(_ => ())

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

    def future: Future[Unit] = snapshotDao
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
