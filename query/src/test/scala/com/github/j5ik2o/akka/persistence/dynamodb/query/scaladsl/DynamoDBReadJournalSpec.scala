/*
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
package com.github.j5ik2o.akka.persistence.dynamodb.query.scaladsl

import akka.actor.{ ActorSystem, Props }
import akka.pattern.ask
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl._
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.Timeout
import com.github.dockerjava.core.DockerClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalPluginConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.WriteJournalDaoImpl
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2.{ V2JournalRowReadDriver, V2JournalRowWriteDriver }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PartitionKeyResolver, SortKeyResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.query.PersistenceTestActor
import com.github.j5ik2o.akka.persistence.dynamodb.query.dao.{ ReadJournalDaoImpl, V2QueryProcessor }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.dockerController.DockerClientConfigUtil
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import java.net.URI
import scala.concurrent.{ ExecutionContextExecutor, Future }
import scala.concurrent.duration._

object DynamoDBReadJournalSpec {
  val dockerClientConfig: DockerClientConfig = DockerClientConfigUtil.buildConfigAwareOfDockerMachine()
  val dynamoDBHost: String                   = DockerClientConfigUtil.dockerHost(dockerClientConfig)
  val dynamoDBPort: Int                      = RandomPortUtil.temporaryServerPort()
}

class DynamoDBReadJournalSpec
    extends TestKit(
      ActorSystem(
        "DynamoDBReadJournalSpec",
        ConfigFactory
          .parseString(
            s"""
             |j5ik2o.dynamo-db-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBReadJournalSpec.dynamoDBHost}:${DynamoDBReadJournalSpec.dynamoDBPort}/"
             |}
             |
             |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
             |  endpoint = "http://${DynamoDBReadJournalSpec.dynamoDBHost}:${DynamoDBReadJournalSpec.dynamoDBPort}/"
             |}
             |
             |j5ik2o.dynamo-db-read-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBReadJournalSpec.dynamoDBHost}:${DynamoDBReadJournalSpec.dynamoDBPort}/"
             |}
             """.stripMargin
          ).withFallback(ConfigFactory.load("query-reference"))
      )
    )
    with AnyFreeSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with BeforeAndAfter
    with BeforeAndAfterAll
    with DynamoDBSpecSupport {

  private val testTimeFactor: Double = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toDouble

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled((30 * testTimeFactor).seconds), interval = scaled((1 * testTimeFactor).seconds))

  override protected lazy val dynamoDBHost: String = DynamoDBReadJournalSpec.dynamoDBHost
  override protected lazy val dynamoDBPort: Int    = DynamoDBReadJournalSpec.dynamoDBPort

  val underlyingAsync: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .httpClient(NettyNioAsyncHttpClient.builder().maxConcurrency(1).build())
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val config = system.settings.config.getConfig("j5ik2o.dynamo-db-journal")

  protected val journalPluginConfig: JournalPluginConfig =
    JournalPluginConfig.fromConfig(config)

  protected val queryPluginConfig: QueryPluginConfig =
    QueryPluginConfig.fromConfig(config)

  private val serialization = SerializationExtension(system)

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, ",", None)

  val queryProcessor =
    new V2QueryProcessor(
      system,
      Some(underlyingAsync),
      None,
      queryPluginConfig,
      Some(new MetricsReporter.None(queryPluginConfig))
    )

  val journalRowReadDriver = new V2JournalRowReadDriver(
    system,
    Some(underlyingAsync),
    None,
    journalPluginConfig,
    Some(new MetricsReporter.None(queryPluginConfig))
  )

  val readJournalDao =
    new ReadJournalDaoImpl(
      queryProcessor,
      journalRowReadDriver,
      queryPluginConfig,
      serializer,
      Some(new MetricsReporter.None(queryPluginConfig))
    )(
      ec,
      system
    )

  val partitionKeyResolver = new PartitionKeyResolver.Default(journalPluginConfig)
  val sortKeyResolver      = new SortKeyResolver.Default(journalPluginConfig)

  val journalRowWriteDriver = new V2JournalRowWriteDriver(
    system,
    Some(underlyingAsync),
    None,
    journalPluginConfig,
    partitionKeyResolver,
    sortKeyResolver,
    Some(new MetricsReporter.None(journalPluginConfig))
  )

  val writeJournalDao =
    new WriteJournalDaoImpl(
      journalPluginConfig,
      journalRowWriteDriver,
      serializer,
      Some(new MetricsReporter.None(journalPluginConfig))
    )(
      ec,
      system
    )

  val readJournal: ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery = {
    PersistenceQuery(system).readJournalFor(DynamoDBReadJournal.Identifier)
  }

  override def afterAll(): Unit = {
    underlyingAsync.close()
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  def countJournal: Long = {
    logger.debug("countJournal: start")
    val numEvents = readJournal
      .currentPersistenceIds().flatMapConcat { pid =>
        readJournal.currentEventsByPersistenceId(pid, 0, Long.MaxValue).map(_ => 1).fold(0)(_ + _)
      }.runFold(0L)(_ + _).futureValue
    logger.debug("==> NumEvents: " + numEvents)
    logger.debug("countJournal: ==> NumEvents: " + numEvents)
    numEvents
  }

  def withPersistenceIds(within: FiniteDuration = 10.second)(f: TestSubscriber.Probe[String] => Unit): Unit = {
    val tp = readJournal
      .persistenceIds().filter { pid =>
        logger.debug(s"withPersistenceIds:filter = $pid")
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
      implicit val to: Timeout = Timeout(30.seconds)
      val pActor               = system.actorOf(Props(new PersistenceTestActor(1)))
      val maxSize              = 500
      val futures = for (n <- 1 to maxSize) yield {
        pActor ? n
      }
      Future.sequence(futures).futureValue
      val results = readJournal.currentEventsByPersistenceId("my-1", 0, Long.MaxValue).runWith(Sink.seq).futureValue
//      println(results)
      results should have size maxSize
      results.map(_.persistenceId).forall(_ == "my-1") shouldBe true
      for (n <- 1 to maxSize) {
        results.map(_.sequenceNr) should contain(n)
      }
    }
    "withPersistenceIds" in withPersistenceIds() { tp =>
      tp.request(Int.MaxValue)
      implicit val to: Timeout = Timeout(10.seconds)
      val pActor               = system.actorOf(Props(new PersistenceTestActor(1)))
      (pActor ? 1).futureValue
      (pActor ? 2).futureValue
      (pActor ? 3).futureValue
      eventually {
        countJournal shouldBe 3
      }
    }
  }

  override protected def afterStartContainers(): Unit = {
    createTable()
  }

  override protected def beforeStopContainers(): Unit = {
    deleteTable()
  }

}
