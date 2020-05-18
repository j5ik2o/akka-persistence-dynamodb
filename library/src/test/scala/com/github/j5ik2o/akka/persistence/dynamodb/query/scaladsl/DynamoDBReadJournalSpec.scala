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
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalPluginConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.WriteJournalDaoImpl
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2.{ V2JournalRowReadDriver, V2JournalRowWriteDriver }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PartitionKeyResolver, SortKeyResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.NullMetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.query.PersistenceTestActor
import com.github.j5ik2o.akka.persistence.dynamodb.query.dao.{ ReadJournalDaoImpl, V2QueryProcessor }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.monix.DynamoDbMonixClient
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfter, FreeSpecLike, Matchers }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.Future
import scala.concurrent.duration._

object DynamoDBReadJournalSpec {
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class DynamoDBReadJournalSpec
    extends TestKit(
      ActorSystem(
        "DynamoDBReadJournalSpec",
        ConfigFactory
          .parseString(
            s"""
             |j5ik2o.dynamo-db-journal.dynamo-db-client {
             |  endpoint = "http://127.0.0.1:${DynamoDBReadJournalSpec.dynamoDBPort}/"
             |}
             |
             |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
             |  endpoint = "http://127.0.0.1:${DynamoDBReadJournalSpec.dynamoDBPort}/"
             |}
             |
             |j5ik2o.dynamo-db-read-journal.dynamo-db-client {
             |  endpoint = "http://127.0.0.1:${DynamoDBReadJournalSpec.dynamoDBPort}/"
             |}
             """.stripMargin
          ).withFallback(ConfigFactory.load())
      )
    )
    with FreeSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with BeforeAndAfter
    with DynamoDBSpecSupport {

  implicit val mat = ActorMaterializer()

  implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBReadJournalSpec.dynamoDBPort

  val underlyingAsync: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .httpClient(NettyNioAsyncHttpClient.builder().maxConcurrency(1).build())
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  implicit val ec = system.dispatcher

  private val config = system.settings.config.getConfig("j5ik2o.dynamo-db-journal")

  protected val journalPluginConfig: JournalPluginConfig =
    JournalPluginConfig.fromConfig(config)

  protected val queryPluginConfig: QueryPluginConfig =
    QueryPluginConfig.fromConfig(config)

  private val serialization = SerializationExtension(system)

  val dynamoDbAsyncClient = DynamoDbAsyncClient(underlyingAsync)
  val taskClient          = DynamoDbMonixClient(dynamoDbAsyncClient)
  val streamClient        = DynamoDbAkkaClient(dynamoDbAsyncClient)

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, ",")

  val queryProcessor =
    new V2QueryProcessor(Some(dynamoDbAsyncClient), None, queryPluginConfig, new NullMetricsReporter)

  val journalRowReadDriver = new V2JournalRowReadDriver(
    Some(dynamoDbAsyncClient),
    None,
    journalPluginConfig,
    new NullMetricsReporter
  )

  val readJournalDao =
    new ReadJournalDaoImpl(
      queryProcessor,
      journalRowReadDriver,
      queryPluginConfig,
      serializer,
      new NullMetricsReporter
    )(
      ec,
      system
    )

  val partitionKeyResolver = new PartitionKeyResolver.Default(config)
  val sortKeyResolver      = new SortKeyResolver.Default(config)

  val journalRowWriteDriver = new V2JournalRowWriteDriver(
    Some(dynamoDbAsyncClient),
    None,
    journalPluginConfig,
    partitionKeyResolver,
    sortKeyResolver,
    new NullMetricsReporter
  )

  val writeJournalDao =
    new WriteJournalDaoImpl(
      journalPluginConfig,
      journalRowWriteDriver,
      serializer,
      new NullMetricsReporter
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
      implicit val to = Timeout(10 seconds)
      val pActor      = system.actorOf(Props(new PersistenceTestActor(1)))
      val maxSize     = 500
      val futures = for (n <- 1 to maxSize) yield {
        pActor ? n
      }
      Future.sequence(futures).futureValue
      val results = readJournal.currentEventsByPersistenceId("my-1", 0, Long.MaxValue).runWith(Sink.seq).futureValue
//      println(results)
      results should have size (maxSize)
      results.map(_.persistenceId).forall(_ == "my-1") shouldBe true
      for (n <- 1 to maxSize) {
        results.map(_.sequenceNr) should contain(n)
      }
    }
    "withPersistenceIds" in withPersistenceIds() { tp =>
      tp.request(Int.MaxValue)
      implicit val to = Timeout(10 seconds)
      val pActor      = system.actorOf(Props(new PersistenceTestActor(1)))
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
