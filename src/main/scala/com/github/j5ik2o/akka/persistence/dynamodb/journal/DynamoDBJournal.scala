package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.net.URI
import java.time.{ Duration => JavaDuration }

import akka.Done
import akka.actor.{ ActorLogging, ActorSystem }
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.SerializationExtension
import akka.stream.{ ActorMaterializer, Materializer }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.{ InPlaceUpdateEvent, WriteFinished }
import com.github.j5ik2o.reactive.dynamodb.DynamoDBAsyncClientV2
import com.github.j5ik2o.reactive.dynamodb.monix.DynamoDBTaskClientV2
import com.typesafe.config.Config
import monix.execution.Scheduler
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.collection.{ immutable, mutable }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

object DynamoDBJournal {

  private case class WriteFinished(pid: String, f: Future[_])

  final case class InPlaceUpdateEvent(persistenceId: String, seqNr: Long, write: AnyRef)

}

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with ActorLogging {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system
  implicit val mat: Materializer    = ActorMaterializer()
  implicit val scheduler: Scheduler = Scheduler(ec)

  protected val persistencePluginConfig: PersistencePluginConfig =
    PersistencePluginConfig.fromConfig(config).getOrElse(PersistencePluginConfig())

  protected val tableName: String = persistencePluginConfig.tableName

  private val httpClientBuilder = {
    val result = NettyNioAsyncHttpClient.builder()
    persistencePluginConfig.clientConfig.foreach { clientConfig =>
      clientConfig.maxConcurrency.foreach(v => result.maxConcurrency(v))
      clientConfig.maxPendingConnectionAcquires.foreach(v => result.maxPendingConnectionAcquires(v))
      clientConfig.readTimeout.foreach(v => result.readTimeout(JavaDuration.ofMillis(v.toMillis)))
      clientConfig.writeTimeout.foreach(v => result.writeTimeout(JavaDuration.ofMillis(v.toMillis)))
      clientConfig.connectionTimeout.foreach(
        v => result.connectionTimeout(JavaDuration.ofMillis(v.toMillis))
      )
      clientConfig.connectionAcquisitionTimeout.foreach(
        v => result.connectionAcquisitionTimeout(JavaDuration.ofMillis(v.toMillis))
      )
      clientConfig.connectionTimeToLive.foreach(
        v => result.connectionTimeToLive(JavaDuration.ofMillis(v.toMillis))
      )
      clientConfig.maxIdleConnectionTimeout.foreach(
        v => result.connectionMaxIdleTime(JavaDuration.ofMillis(v.toMillis))
      )
      clientConfig.useConnectionReaper.foreach(v => result.useIdleConnectionReaper(v))
      clientConfig.userHttp2.foreach(
        v => if (v) result.protocol(Protocol.HTTP2) else result.protocol(Protocol.HTTP1_1)
      )
      clientConfig.maxHttp2Streams.foreach(v => result.maxHttp2Streams(v))
    }
    result
  }
  private var dynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder().httpClient(httpClientBuilder.build)
  persistencePluginConfig.clientConfig.foreach { clientConfig =>
    (clientConfig.accessKeyId, clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        dynamoDbAsyncClientBuilder = dynamoDbAsyncClientBuilder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
    }
    clientConfig.endpoint.foreach { ep =>
      dynamoDbAsyncClientBuilder = dynamoDbAsyncClientBuilder.endpointOverride(URI.create(ep))
    }
  }
  protected val underlying: DynamoDbAsyncClient = dynamoDbAsyncClientBuilder.build()
  protected val client: DynamoDBTaskClientV2    = DynamoDBTaskClientV2(DynamoDBAsyncClientV2(underlying))
  protected val journalDao: JournalDao with JournalDaoWithUpdates =
    new JournalDaoImpl(client, SerializationExtension(system), persistencePluginConfig)

  private val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val future        = journalDao.asyncWriteMessages(messages).runToFuture
    val persistenceId = messages.head.persistenceId
    writeInProgress.put(persistenceId, future)
    future.onComplete(_ => self ! WriteFinished(persistenceId, future))
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    journalDao.delete(persistenceId, toSequenceNr).runToFuture
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] =
    journalDao
      .messages(persistenceId, fromSequenceNr, toSequenceNr, max)
      .runForeach(recoveryCallback)
      .map(_ => ())

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    def fetchHighestSeqNr(): Future[Long] =
      journalDao.highestSequenceNr(persistenceId, fromSequenceNr).runToFuture
    writeInProgress.get(persistenceId) match {
      case None    => fetchHighestSeqNr()
      case Some(f) =>
        // we must fetch the highest sequence number after the previous write has completed
        // If the previous write failed then we can ignore this
        f.recover { case _ => () }.flatMap(_ => fetchHighestSeqNr())
    }
  }

  private def asyncUpdateEvent(persistenceId: String, sequenceNr: Long, message: AnyRef): Future[Done] = {
    journalDao.update(persistenceId, sequenceNr, message).runToFuture.map(_ => Done)
  }

  override def postStop(): Unit = {
    underlying.close()
    super.postStop()
  }

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, _) =>
      writeInProgress.remove(persistenceId)
    case InPlaceUpdateEvent(pid, seq, write) =>
      asyncUpdateEvent(pid, seq, write).pipeTo(sender())
  }
}
