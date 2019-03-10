package com.github.j5ik2o.akka.persistence.dynamodb.journal
import java.net.URI

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.dynamodb.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.config.{JournalPluginConfig, QueryPluginConfig}
import com.github.j5ik2o.akka.persistence.dynamodb.query.dao.ReadJournalDaoImpl
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDBStreamClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.model._
import com.github.j5ik2o.reactive.aws.dynamodb.monix.DynamoDBTaskClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.{DynamoDBAsyncClientV2, DynamoDBEmbeddedSpecSupport, DynamoDBSyncClientV2}
import monix.execution.Scheduler
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpecLike, Matchers}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbClient}

import scala.concurrent.duration._

class WriteJournalDaoImplSpec
    extends TestKit(ActorSystem("ReadJournalDaoImplSpec"))
    with FreeSpecLike
    with Matchers
    with ScalaFutures
    with DynamoDBEmbeddedSpecSupport {
  implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  val underlyingAsync: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .httpClient(NettyNioAsyncHttpClient.builder().maxConcurrency(1).build())
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  val underlyingSync: DynamoDbClient = DynamoDbClient
    .builder()
    .httpClient(ApacheHttpClient.builder().maxConnections(1).build())
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  ApacheHttpClient.builder().maxConnections(1).build()

  import scala.concurrent.ExecutionContext.Implicits.global

  val asyncClient  = DynamoDBAsyncClientV2(underlyingAsync)
  val syncClient   = DynamoDBSyncClientV2(underlyingSync)
  val taskClient   = DynamoDBTaskClientV2(asyncClient)
  val streamClient = DynamoDBStreamClientV2(asyncClient)

  private val serialization = SerializationExtension(system)

  protected val journalPluginConfig: JournalPluginConfig =
    JournalPluginConfig.fromConfig(system.settings.config)
  protected val queryPluginConfig: QueryPluginConfig =
    QueryPluginConfig.fromConfig(system.settings.config)

  implicit val mat = ActorMaterializer()
  implicit val ec  = system.dispatcher

  val readJournalDao  = new ReadJournalDaoImpl(asyncClient, serialization, queryPluginConfig)(ec)
  val writeJournalDao = new WriteJournalDaoImpl(asyncClient, serialization, journalPluginConfig)(ec, mat)

  val sch = Scheduler(ec)

  "WriteJournalDaoImpl" - {
    "write" in {
      val max = 60
      val journalRows = (1 to max).map { n =>
        JournalRow("a-" + n.toString, 1, deleted = false, Array(1.toByte, 2.toByte, 3.toByte), Long.MaxValue)
      }
      val result = writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      result shouldBe max
    }
    "write-read" in {
      val pid = "b-1"
      val max = 60
      val journalRows = (1 to max).map { n =>
        JournalRow(pid, n, deleted = false, Array(1.toByte, 2.toByte, 3.toByte), Long.MaxValue)
      }
      val result = writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      result shouldBe max
      val results =
        writeJournalDao.getMessages(pid, 1, 60, Long.MaxValue).runFold(Seq.empty[JournalRow])(_ :+ _).futureValue
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
        JournalRow(pid, n, deleted = false, Array(1.toByte, 2.toByte, 3.toByte), Long.MaxValue)
      }
      val result = writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      result shouldBe max
      writeJournalDao.updateMessage(journalRows.head.withDeleted).runWith(Sink.head).futureValue
      val results =
        writeJournalDao.getMessages(pid, 1, 60, Long.MaxValue, None).runFold(Seq.empty[JournalRow])(_ :+ _).futureValue
      results.head.persistenceId shouldBe pid
      results.head.sequenceNumber shouldBe 1
      results.head.deleted shouldBe true
    }
    "delete" in {
      val pid = "d-1"
      val max = 60
      val journalRows = (1 to max).map { n =>
        JournalRow(pid, n, deleted = false, Array(1.toByte, 2.toByte, 3.toByte), Long.MaxValue)
      }
      val result = writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      result shouldBe max
      writeJournalDao.deleteMessages(pid, 60).runWith(Sink.head).futureValue
      val results =
        writeJournalDao.getMessages(pid, 1, 60, Long.MaxValue).runFold(Seq.empty[JournalRow])(_ :+ _).futureValue
      results.size shouldBe 0
    }
  }
  override def beforeAll(): Unit = {
    super.beforeAll()
    val tableName = "Journal"
    val createRequest = CreateTableRequest()
      .withAttributeDefinitions(
        Some(
          Seq(
            AttributeDefinition()
              .withAttributeName(Some("persistence-id"))
              .withAttributeType(Some(AttributeType.S)),
            AttributeDefinition()
              .withAttributeName(Some("sequence-nr"))
              .withAttributeType(Some(AttributeType.N))
          )
        )
      )
      .withKeySchema(
        Some(
          Seq(
            KeySchemaElement()
              .withAttributeName(Some("persistence-id"))
              .withKeyType(Some(KeyType.HASH)),
            KeySchemaElement()
              .withAttributeName(Some("sequence-nr"))
              .withKeyType(Some(KeyType.RANGE))
          )
        )
      )
      .withProvisionedThroughput(
        Some(
          ProvisionedThroughput()
            .withReadCapacityUnits(Some(10L))
            .withWriteCapacityUnits(Some(10L))
        )
      )
      .withTableName(Some(tableName))
    val createResponse = asyncClient
      .createTable(createRequest).futureValue
    createResponse.isSuccessful shouldBe true
  }
}
