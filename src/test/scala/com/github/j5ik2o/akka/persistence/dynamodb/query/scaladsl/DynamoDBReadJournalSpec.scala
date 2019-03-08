package com.github.j5ik2o.akka.persistence.dynamodb.query.scaladsl

import java.net.URI

import akka.actor.{ ActorSystem, Props }
import akka.pattern.ask
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl._
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.dynamodb.config.PersistencePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{
  ByteArrayJournalSerializer,
  JournalRow,
  WriteJournalDaoImpl
}
import com.github.j5ik2o.akka.persistence.dynamodb.query.TestActor
import com.github.j5ik2o.akka.persistence.dynamodb.query.dao.ReadJournalDaoImpl
import com.github.j5ik2o.akka.persistence.dynamodb.query.query.DynamoDBSpecSupport
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDBStreamClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.monix.DynamoDBTaskClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDBAsyncClientV2, DynamoDBSyncClientV2 }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfter, FreeSpecLike, Matchers }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

import scala.concurrent.duration._

class DynamoDBReadJournalSpec
    extends TestKit(ActorSystem("DynamoDBReadJournalSpec", ConfigFactory.load("default.conf")))
    with FreeSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with BeforeAndAfter
    with DynamoDBSpecSupport {

  implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = 8000

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

  implicit val mat = ActorMaterializer()
  implicit val ec  = system.dispatcher

  private val config = system.settings.config

  protected val persistencePluginConfig: PersistencePluginConfig =
    PersistencePluginConfig.fromConfig(config)

  private val serialization = SerializationExtension(system)

  val asyncClient: DynamoDBAsyncClientV2 = DynamoDBAsyncClientV2(underlyingAsync)
  val syncClient                         = DynamoDBSyncClientV2(underlyingSync)
  val taskClient                         = DynamoDBTaskClientV2(asyncClient)
  val streamClient                       = DynamoDBStreamClientV2(asyncClient)
  val readJournalDao                     = new ReadJournalDaoImpl(asyncClient, syncClient, serialization, persistencePluginConfig)(ec)
  val writeJournalDao                    = new WriteJournalDaoImpl(asyncClient, serialization, persistencePluginConfig)(ec, mat)
  val readJournal: ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery = {
    PersistenceQuery(system).readJournalFor(DynamoDBReadJournal.Identifier)
  }

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, persistencePluginConfig.tagSeparator)

  override def afterAll(): Unit = {
    underlyingAsync.close()
    underlyingSync.close()
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  def countJournal: Long = {
    logger.info("countJournal: start")
    val numEvents = readJournal
      .currentPersistenceIds().flatMapConcat { pid =>
        logger.info(s">>>>> pid = $pid")
        readJournal.currentEventsByPersistenceId(pid, 0, Long.MaxValue).map(_ => 1).fold(0)(_ + _)
      }.runFold(0L)(_ + _).futureValue
    logger.info("==> NumEvents: " + numEvents)
    logger.info("countJournal: ==> NumEvents: " + numEvents)
    numEvents
  }

  def withPersistenceIds(within: FiniteDuration = 10.second)(f: TestSubscriber.Probe[String] => Unit): Unit = {
    val tp = readJournal
      .persistenceIds().filter { pid =>
        logger.info(s"withPersistenceIds:filter = $pid")
        (1 to 3).map(id => s"my-$id").contains(pid)
      }.runWith(TestSink.probe[String])
    tp.within(within)(f(tp))
  }

  "DynamoDBReadJournalSpec" - {
//    "currentPersistenceIds" in {
//      val journalRows = Seq(
//        JournalRow("a-1", 1, deleted = false, "ABC".getBytes(), Long.MaxValue),
//        JournalRow("a-1", 2, deleted = false, "ABC".getBytes(), Long.MaxValue),
//        JournalRow("b-1", 1, deleted = false, "ABC".getBytes(), Long.MaxValue)
//      )
//      writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
//      val sub =
//        readJournal.currentPersistenceIds().toMat(TestSink.probe[String])(Keep.right).run()
//      sub.request(Long.MaxValue)
//      journalRows.map(_.persistenceId) should contain(sub.expectNext())
//      journalRows.map(_.persistenceId) should contain(sub.expectNext())
//      sub.expectNoMessage(100.millis)
//      // sub.expectComplete()
//    }
    "currentEventsByPersistenceId" in {
      implicit val to = Timeout(10 seconds)
      val pActor      = system.actorOf(Props(new TestActor(1)))
      (pActor ? 1).futureValue
      (pActor ? 2).futureValue
      (pActor ? 3).futureValue
      val results = readJournal.currentEventsByPersistenceId("my-1", 0, Long.MaxValue).runWith(Sink.seq).futureValue
      println(results)
      results should have size (3)
      results.map(_.persistenceId).forall(_ == "my-1") shouldBe true
      results.map(_.sequenceNr) should contain(1)
      results.map(_.sequenceNr) should contain(2)
      results.map(_.sequenceNr) should contain(3)
    }
    "withPersistenceIds" in withPersistenceIds() { tp =>
      tp.request(Int.MaxValue)
      implicit val to = Timeout(10 seconds)
      val pActor      = system.actorOf(Props(new TestActor(1)))
      (pActor ? 1).futureValue
      (pActor ? 2).futureValue
      (pActor ? 3).futureValue
      eventually {
        countJournal shouldBe 3
      }
    }
  }

  before { createTable }

  after { deleteTable }

}
