package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.Done
import akka.actor.{ ActorLogging, ActorSystem }
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Sink
import akka.stream.{ ActorMaterializer, Materializer }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PersistencePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.{ InPlaceUpdateEvent, WriteFinished }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer
import com.github.j5ik2o.akka.persistence.dynamodb.{ DynamoDbClientBuilderUtils, HttpClientUtils }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import com.typesafe.config.Config
import monix.execution.Scheduler
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.collection.{ immutable, mutable }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

object DynamoDBJournal {

  final case class InPlaceUpdateEvent(persistenceId: String, seqNr: Long, write: AnyRef)

  private case class WriteFinished(pid: String, f: Future[_])

}

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with ActorLogging {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system
  implicit val mat: Materializer    = ActorMaterializer()
  implicit val scheduler: Scheduler = Scheduler(ec)

  protected val persistencePluginConfig: PersistencePluginConfig = PersistencePluginConfig.fromConfig(config)

  protected val tableName: String = persistencePluginConfig.tableName
  private val httpClientBuilder   = HttpClientUtils.asyncBuilder(persistencePluginConfig)
  private val dynamoDbAsyncClientBuilder =
    DynamoDbClientBuilderUtils.asyncBuilder(persistencePluginConfig, httpClientBuilder.build())
  private val serialization = SerializationExtension(system)

  protected val javaClient: DynamoDbAsyncClient    = dynamoDbAsyncClientBuilder.build()
  protected val asyncClient: DynamoDBAsyncClientV2 = DynamoDBAsyncClientV2(javaClient)

  protected val journalDao: WriteJournalDao with WriteJournalDaoWithUpdates =
    new WriteJournalDaoImpl(asyncClient, serialization, persistencePluginConfig)

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, persistencePluginConfig.tagSeparator)
  private val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val serializedTries: Seq[Either[Throwable, Seq[JournalRow]]] = serializer.serialize(messages)
    val rowsToWrite: Seq[JournalRow] = for {
      serializeTry <- serializedTries
      row          <- serializeTry.getOrElse(Seq.empty)
    } yield row
    def resultWhenWriteComplete: Seq[Either[Throwable, Unit]] =
      if (serializedTries.forall(_.isRight)) Nil
      else
        serializedTries
          .map(_.map(_ => ()))
    val future =
      journalDao
        .putMessages(rowsToWrite).runWith(Sink.head).map(
          _ => resultWhenWriteComplete.map(_.toTry).to
        )
    val persistenceId = messages.head.persistenceId
    writeInProgress.put(persistenceId, future)
    future.onComplete(_ => self ! WriteFinished(persistenceId, future))
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    journalDao.deleteMessages(persistenceId, toSequenceNr).runWith(Sink.head)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] =
    journalDao
      .getMessages(persistenceId, fromSequenceNr, toSequenceNr, max)
      .via(serializer.deserializeFlowWithoutTags)
      .runForeach(recoveryCallback)
      .map(_ => ())

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    def fetchHighestSeqNr(): Future[Long] =
      journalDao.highestSequenceNr(persistenceId, fromSequenceNr).runWith(Sink.head)

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
    case InPlaceUpdateEvent(pid, seq, write) =>
      asyncUpdateEvent(pid, seq, write).pipeTo(sender())
  }

  private def asyncUpdateEvent(persistenceId: String, sequenceNr: Long, message: AnyRef): Future[Done] = {
    val write = PersistentRepr(message, sequenceNr, persistenceId)
    val serializedRow: JournalRow = serializer.serialize(write) match {
      case Right(t) => t
      case Left(ex) =>
        throw new IllegalArgumentException(
          s"Failed to serialize ${write.getClass} for update of [$persistenceId] @ [$sequenceNr]"
        )
    }
    journalDao.updateMessage(serializedRow).runWith(Sink.ignore)
  }
}
