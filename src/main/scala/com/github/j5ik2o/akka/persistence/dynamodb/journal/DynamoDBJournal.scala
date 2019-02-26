package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.time.{ Duration => JavaDuration }

import akka.Done
import akka.actor.ActorSystem
import akka.pattern.pipe
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.SerializationExtension
import akka.stream.{ ActorMaterializer, Materializer }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.{ InPlaceUpdateEvent, WriteFinished }
import com.github.j5ik2o.reactive.dynamodb.DynamoDBAsyncClientV2
import com.typesafe.config.Config
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

class DynamoDBJournal(config: Config) extends AsyncWriteJournal {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system
  implicit val mat: Materializer    = ActorMaterializer()

  private val dynamoDBConfig = pureconfig.loadConfigOrThrow[DynamoDBConfig](config)

  private val httpClientBuilder = {
    val builder = NettyNioAsyncHttpClient.builder()
    dynamoDBConfig.maxConcurrency.foreach(v => builder.maxConcurrency(v))
    dynamoDBConfig.maxPendingConnectionAcquires.foreach(v => builder.maxPendingConnectionAcquires(v))
    dynamoDBConfig.readTimeout.foreach(v => builder.readTimeout(JavaDuration.ofMillis(v.toMillis)))
    dynamoDBConfig.writeTimeout.foreach(v => builder.writeTimeout(JavaDuration.ofMillis(v.toMillis)))
    dynamoDBConfig.connectionTimeout.foreach(
      v => builder.connectionTimeout(JavaDuration.ofMillis(v.toMillis))
    )
    dynamoDBConfig.connectionAcquisitionTimeout.foreach(
      v => builder.connectionAcquisitionTimeout(JavaDuration.ofMillis(v.toMillis))
    )
    dynamoDBConfig.connectionTimeToLive.foreach(
      v => builder.connectionTimeToLive(JavaDuration.ofMillis(v.toMillis))
    )
    dynamoDBConfig.maxIdleConnectionTimeout.foreach(
      v => builder.connectionMaxIdleTime(JavaDuration.ofMillis(v.toMillis))
    )
    dynamoDBConfig.useConnectionReaper.foreach(v => builder.useIdleConnectionReaper(v))
    dynamoDBConfig.userHttp2.foreach(
      v => if (v) builder.protocol(Protocol.HTTP2) else builder.protocol(Protocol.HTTP1_1)
    )
    dynamoDBConfig.maxHttp2Streams.foreach(v => builder.maxHttp2Streams(v))
    builder
  }

  private val underlying = DynamoDbAsyncClient.builder().httpClient(httpClientBuilder.build).build()
  private val client     = DynamoDBAsyncClientV2(underlying)
  private val journalDao =
    new JournalDaoImpl(client, dynamoDBConfig.tableName, SerializationExtension(system), dynamoDBConfig.tagSeparator)

  private val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val future        = journalDao.asyncWriteMessages(messages)
    val persistenceId = messages.head.persistenceId
    writeInProgress.put(persistenceId, future)
    future.onComplete(_ => self ! WriteFinished(persistenceId, future))
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    journalDao.delete(persistenceId, toSequenceNr)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] =
    journalDao
      .messages(persistenceId, fromSequenceNr, toSequenceNr, max)
      .mapAsync(1)(deserializedRepr => Future.fromTry(deserializedRepr.toTry))
      .runForeach(recoveryCallback)
      .map(_ => ())

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    def fetchHighestSeqNr() = journalDao.highestSequenceNr(persistenceId, fromSequenceNr)
    writeInProgress.get(persistenceId) match {
      case None    => fetchHighestSeqNr()
      case Some(f) =>
        // we must fetch the highest sequence number after the previous write has completed
        // If the previous write failed then we can ignore this
        f.recover { case _ => () }.flatMap(_ => fetchHighestSeqNr())
    }
  }

  private def asyncUpdateEvent(persistenceId: String, sequenceNr: Long, message: AnyRef): Future[Done] = {
    journalDao.update(persistenceId, sequenceNr, message)
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
