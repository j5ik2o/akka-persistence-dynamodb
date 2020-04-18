package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.net.URI

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalPluginConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.NullMetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.query.dao.ReadJournalDaoImpl
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamoDBSpecSupport
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.reactive.aws.dynamodb.monix.DynamoDbMonixClient
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDbAsyncClient, DynamoDbSyncClient }
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import org.scalatest.{ FreeSpecLike, Matchers }
import org.scalatest.concurrent.ScalaFutures
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbClient
}

import scala.concurrent.duration._

class WriteJournalDaoImplSpec
    extends TestKit(ActorSystem("WriteJournalDaoImplSpec", ConfigFactory.load()))
    with FreeSpecLike
    with Matchers
    with ScalaFutures
    with DynamoDBSpecSupport {
  implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  implicit val mat = ActorMaterializer()

  val underlyingAsync: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  val underlyingSync: JavaDynamoDbClient = JavaDynamoDbClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.WriteJournalDaoImpl

  val asyncClient  = DynamoDbAsyncClient(underlyingAsync)
  val syncClient   = DynamoDbSyncClient(underlyingSync)
  val taskClient   = DynamoDbMonixClient(asyncClient)
  val streamClient = DynamoDbAkkaClient(asyncClient)

  private val serialization = SerializationExtension(system)

  private val journalPluginConfig: JournalPluginConfig =
    JournalPluginConfig.fromConfig(system.settings.config.asConfig("j5ik2o.dynamo-db-journal"))

  private val queryPluginConfig: QueryPluginConfig =
    QueryPluginConfig.fromConfig(system.settings.config.asConfig("j5ik2o.dynamo-db-read-journal"))

  implicit val ec = system.dispatcher

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, ",")

  val readJournalDao =
    new ReadJournalDaoImpl(asyncClient, serialization, queryPluginConfig, serializer, new NullMetricsReporter)(
      ec,
      system
    )
  val config = system.settings.config.getConfig("j5ik2o.dynamo-db-journal")

  val partitionKeyResolver = new PartitionKeyResolver.Default(config)

  val writeJournalDao =
    new WriteJournalDaoImpl(
      asyncClient,
      serialization,
      journalPluginConfig,
      partitionKeyResolver,
      serializer,
      new NullMetricsReporter
    )(
      ec,
      system
    )

  val sch = Scheduler(ec)

  "WriteJournalDaoImpl" - {
    "write" in {
      val max = 60
      val journalRows = (1 to max).map { n =>
        JournalRow(
          PersistenceId("a-" + n.toString),
          SequenceNumber(1L),
          deleted = false,
          Array(1.toByte, 2.toByte, 3.toByte),
          Long.MaxValue
        )
      }
      val result = writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      result shouldBe max
    }
    "write-read" in {
      val pid = "b-1"
      val max = 60
      val journalRows = (1 to max).map { n =>
        JournalRow(
          PersistenceId(pid),
          SequenceNumber(n),
          deleted = false,
          Array(1.toByte, 2.toByte, 3.toByte),
          Long.MaxValue
        )
      }
      val result = writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      result shouldBe max
      val results =
        writeJournalDao
          .getMessagesAsJournalRow(PersistenceId(pid), SequenceNumber(1), SequenceNumber(60), Long.MaxValue).runFold(
            Seq.empty[JournalRow]
          )(_ :+ _).futureValue
      results.size shouldBe 60
      results.toVector.zip(journalRows.toVector).foreach {
        case (v1, v2) =>
          v1.persistenceId shouldBe v2.persistenceId
          v1.sequenceNumber shouldBe v2.sequenceNumber
          v1.deleted shouldBe v2.deleted
          v1.ordering shouldBe v2.ordering
          v1.tags shouldBe v2.tags
          (v1.message sameElements v2.message) shouldBe true
      }
    }
    "update" in {
      val pid = "c-1"
      val max = 60
      val journalRows = (1 to max).map { n =>
        JournalRow(
          PersistenceId(pid),
          SequenceNumber(n),
          deleted = false,
          Array(1.toByte, 2.toByte, 3.toByte),
          Long.MaxValue
        )
      }
      val result = writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      result shouldBe max
      writeJournalDao.updateMessage(journalRows.head.withDeleted).runWith(Sink.head).futureValue
      val results =
        writeJournalDao
          .getMessagesAsJournalRow(PersistenceId(pid), SequenceNumber(1), SequenceNumber(60), Long.MaxValue, None).runFold(
            Seq.empty[JournalRow]
          )(_ :+ _).futureValue
      results.head.persistenceId shouldBe PersistenceId(pid)
      results.head.sequenceNumber shouldBe SequenceNumber(1)
      results.head.deleted shouldBe true
    }
    "delete" in {
      val pid = "d-1"
      val max = 60
      val journalRows = (1 to max).map { n =>
        JournalRow(
          PersistenceId(pid),
          SequenceNumber(n),
          deleted = false,
          Array(1.toByte, 2.toByte, 3.toByte),
          Long.MaxValue
        )
      }
      val result = writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      result shouldBe max
      writeJournalDao
        .deleteMessages(PersistenceId(pid), SequenceNumber(60)).runWith(Sink.head).futureValue
      val results =
        writeJournalDao
          .getMessagesAsJournalRow(PersistenceId(pid), SequenceNumber(1), SequenceNumber(60), Long.MaxValue).runFold(
            Seq.empty[JournalRow]
          )(_ :+ _).futureValue
      results.size shouldBe 0
    }
  }

  before { createTable }

  after { deleteTable }

}
