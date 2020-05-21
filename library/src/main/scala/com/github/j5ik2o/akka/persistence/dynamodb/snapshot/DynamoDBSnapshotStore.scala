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
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.{ SnapshotDao, SnapshotDaoImpl }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.V2DynamoDbClientBuilderUtils
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.{ ExecutionContext, Future }

object DynamoDBSnapshotStore {

  def toSelectedSnapshot(tupled: (SnapshotMetadata, Any)): SelectedSnapshot = tupled match {
    case (meta: SnapshotMetadata, snapshot: Any) => SelectedSnapshot(meta, snapshot)
  }
}

class DynamoDBSnapshotStore(config: Config) extends SnapshotStore {
  import DynamoDBSnapshotStore._

  implicit val ec: ExecutionContext        = context.dispatcher
  implicit val system: ExtendedActorSystem = context.system.asInstanceOf[ExtendedActorSystem]
  implicit val mat                         = ActorMaterializer()

  private val serialization                        = SerializationExtension(system)
  protected val pluginConfig: SnapshotPluginConfig = SnapshotPluginConfig.fromConfig(config)

  protected val javaClient: JavaDynamoDbAsyncClient =
    V2DynamoDbClientBuilderUtils.setupAsync(system.dynamicAccess, pluginConfig).build()
  protected val asyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(javaClient)

  protected val snapshotDao: SnapshotDao =
    new SnapshotDaoImpl(asyncClient, serialization, pluginConfig)

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] = {
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

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    snapshotDao.save(metadata, snapshot).runWith(Sink.ignore).map(_ => ())

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    snapshotDao
      .delete(PersistenceId(metadata.persistenceId), SequenceNumber(metadata.sequenceNr)).map(_ => ()).runWith(
        Sink.ignore
      ).map(_ => ())

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val pid = PersistenceId(persistenceId)
    criteria match {
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
  }

}
