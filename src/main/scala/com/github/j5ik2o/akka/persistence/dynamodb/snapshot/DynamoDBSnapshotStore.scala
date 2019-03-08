package com.github.j5ik2o.akka.persistence.dynamodb.snapshot
import akka.actor.ActorSystem
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.{ ActorMaterializer, Materializer }
import com.github.j5ik2o.akka.persistence.dynamodb.{ DynamoDbClientBuilderUtils, HttpClientUtils }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PersistencePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao.{ SnapshotDao, SnapshotDaoImpl }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.{ ExecutionContext, Future }

object DynamoDBSnapshotStore {
  def toSelectedSnapshot(tupled: (SnapshotMetadata, Any)): SelectedSnapshot = tupled match {
    case (meta: SnapshotMetadata, snapshot: Any) => SelectedSnapshot(meta, snapshot)
  }
}

class DynamoDBSnapshotStore(config: Config) extends SnapshotStore {
  import DynamoDBSnapshotStore._

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system
  implicit val mat: Materializer    = ActorMaterializer()

  private val serialization                                      = SerializationExtension(system)
  protected val persistencePluginConfig: PersistencePluginConfig = PersistencePluginConfig.fromConfig(config)

  private val httpClientBuilder = HttpClientUtils.asyncBuilder(persistencePluginConfig)
  private val dynamoDbAsyncClientBuilder =
    DynamoDbClientBuilderUtils.asyncBuilder(persistencePluginConfig, httpClientBuilder.build())
  protected val javaClient: DynamoDbAsyncClient    = dynamoDbAsyncClientBuilder.build()
  protected val asyncClient: DynamoDBAsyncClientV2 = DynamoDBAsyncClientV2(javaClient)
  protected val snapshotDao: SnapshotDao =
    new SnapshotDaoImpl(asyncClient,
                        serialization,
                        persistencePluginConfig.journalTableName,
                        persistencePluginConfig.parallelism,
                        persistencePluginConfig.batchSize)

  override def loadAsync(persistenceId: String,
                         criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val result = criteria match {
      case SnapshotSelectionCriteria(Long.MaxValue, Long.MaxValue, _, _) =>
        snapshotDao.latestSnapshot(persistenceId)
      case SnapshotSelectionCriteria(Long.MaxValue, maxTimestamp, _, _) =>
        snapshotDao.snapshotForMaxTimestamp(persistenceId, maxTimestamp)
      case SnapshotSelectionCriteria(maxSequenceNr, Long.MaxValue, _, _) =>
        snapshotDao.snapshotForMaxSequenceNr(persistenceId, maxSequenceNr)
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, _, _) =>
        snapshotDao.snapshotForMaxSequenceNrAndMaxTimestamp(persistenceId, maxSequenceNr, maxTimestamp)
      case _ => Source.empty
    }
    result.map(_.map(toSelectedSnapshot)).runWith(Sink.head)
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    snapshotDao.save(metadata, snapshot).runWith(Sink.ignore).map(_ => ())

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    snapshotDao.delete(metadata.persistenceId, metadata.sequenceNr).map(_ => ()).runWith(Sink.ignore).map(_ => ())

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = criteria match {
    case SnapshotSelectionCriteria(Long.MaxValue, Long.MaxValue, _, _) =>
      snapshotDao.deleteAllSnapshots(persistenceId).runWith(Sink.ignore).map(_ => ())
    case SnapshotSelectionCriteria(Long.MaxValue, maxTimestamp, _, _) =>
      snapshotDao.deleteUpToMaxTimestamp(persistenceId, maxTimestamp).runWith(Sink.ignore).map(_ => ())
    case SnapshotSelectionCriteria(maxSequenceNr, Long.MaxValue, _, _) =>
      snapshotDao.deleteUpToMaxSequenceNr(persistenceId, maxSequenceNr).runWith(Sink.ignore).map(_ => ())
    case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, _, _) =>
      snapshotDao
        .deleteUpToMaxSequenceNrAndMaxTimestamp(persistenceId, maxSequenceNr, maxTimestamp).runWith(Sink.ignore).map(
          _ => ()
        )
    case _ => Future.successful(())
  }

}
