package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import java.net.URI

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.dynamodb.config.PersistencePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, WriteJournalDaoImpl }
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDBStreamClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.model._
import com.github.j5ik2o.reactive.aws.dynamodb.monix.DynamoDBTaskClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.{
  DynamoDBAsyncClientV2,
  DynamoDBEmbeddedSpecSupport,
  DynamoDBSyncClientV2
}
import monix.execution.Scheduler
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FreeSpecLike, Matchers }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class ReadJournalDaoImplSpec
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

  import scala.concurrent.ExecutionContext.Implicits.global

  val asyncClient  = DynamoDBAsyncClientV2(underlyingAsync)
  val syncClient   = DynamoDBSyncClientV2(underlyingSync)
  val taskClient   = DynamoDBTaskClientV2(asyncClient)
  val streamClient = DynamoDBStreamClientV2(asyncClient)

  private val serialization = SerializationExtension(system)
  protected val persistencePluginConfig: PersistencePluginConfig =
    PersistencePluginConfig.fromConfig(system.settings.config)

  implicit val mat    = ActorMaterializer()
  implicit val ec     = system.dispatcher
  val readJournalDao  = new ReadJournalDaoImpl(asyncClient, syncClient, serialization, persistencePluginConfig)(ec)
  val writeJournalDao = new WriteJournalDaoImpl(asyncClient, serialization, persistencePluginConfig)(ec, mat)

  val sch = Scheduler(ec)

  "ReadJournalDaoImplSpec" - {
    "allPersistenceIdsSource" in {
      val journalRows = (1 to 100).map { n =>
        JournalRow(n.toString, 1, deleted = false, "ABC".getBytes(), Long.MaxValue)
      }
      writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      val result = readJournalDao
        .allPersistenceIdsSource(journalRows.size).runWith(Sink.seq).futureValue
      val excepted = journalRows.map(_.persistenceId).toList
      result should contain theSameElementsAs excepted
    }
    "getMessages" in {
      val pid = "a-1"
      val journalRows = (1 to 100).map { n =>
        JournalRow(pid, n, deleted = false, "ABC".getBytes(), Long.MaxValue)
      }
      writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      val result = readJournalDao.getMessages(pid, 1, 1000, Long.MaxValue).runWith(Sink.seq).futureValue
      result.map(v => (v.persistenceId, v.sequenceNumber, v.deleted)) should contain theSameElementsAs journalRows.map(
        v => (v.persistenceId, v.sequenceNumber, v.deleted)
      )
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
