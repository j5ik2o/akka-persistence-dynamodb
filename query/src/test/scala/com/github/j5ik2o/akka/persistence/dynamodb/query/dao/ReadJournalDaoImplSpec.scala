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
package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import java.net.URI

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalPluginConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2.{ V2JournalRowReadDriver, V2JournalRowWriteDriver }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamoDBSpecSupport
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.monix.DynamoDbMonixClient
import com.typesafe.config.{ Config, ConfigFactory }
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FreeSpecLike, Matchers }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.duration._

class ReadJournalDaoImplSpec
    extends TestKit(ActorSystem("ReadJournalDaoImplSpec", ConfigFactory.load()))
    with FreeSpecLike
    with Matchers
    with ScalaFutures
    with DynamoDBSpecSupport {

  implicit val mat = ActorMaterializer()

  implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  val underlyingAsync: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.WriteJournalDaoImpl

  val dynamoDbAsyncClient = DynamoDbAsyncClient(underlyingAsync)
  val taskClient          = DynamoDbMonixClient(dynamoDbAsyncClient)
  val streamClient        = DynamoDbAkkaClient(dynamoDbAsyncClient)

  private val serialization = SerializationExtension(system)

  private val journalPluginConfig: JournalPluginConfig =
    JournalPluginConfig.fromConfig(system.settings.config.getOrElse[Config]("dynamo-db-journal", ConfigFactory.empty()))

  private val queryPluginConfig: QueryPluginConfig =
    QueryPluginConfig.fromConfig(
      system.settings.config.getOrElse[Config]("dynamo-db-read-journal", ConfigFactory.empty())
    )

  implicit val ec = system.dispatcher

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, ",")

  val queryProcessor =
    new V2QueryProcessor(
      Some(dynamoDbAsyncClient),
      None,
      queryPluginConfig,
      Some(new MetricsReporter.None(queryPluginConfig))
    )

  val journalRowReadDriver = new V2JournalRowReadDriver(
    system,
    Some(dynamoDbAsyncClient),
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

  val config = system.settings.config.getConfig("j5ik2o.dynamo-db-journal")

  val partitionKeyResolver = new PartitionKeyResolver.Default(journalPluginConfig)
  val sortKeyResolver      = new SortKeyResolver.Default(journalPluginConfig)

  val journalRowWriteDriver = new V2JournalRowWriteDriver(
    system,
    Some(dynamoDbAsyncClient),
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

  val sch = Scheduler(ec)

  "ReadJournalDaoImpl" - {
    "allPersistenceIds" in {
      val journalRows = (1 to 100).map { n =>
        JournalRow(
          PersistenceId("a-" + n.toString),
          SequenceNumber(1),
          deleted = false,
          "ABC".getBytes(),
          Long.MaxValue
        )
      }
      writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      val result = readJournalDao
        .allPersistenceIds(journalRows.size).runWith(Sink.seq).futureValue
      val excepted = journalRows.map(_.persistenceId).toList
      result should contain theSameElementsAs excepted
    }
    "getMessages" in {
      val pid = "a-1"
      val journalRows = (1 to 100).map { n =>
        JournalRow(PersistenceId(pid), SequenceNumber(n), deleted = false, "ABC".getBytes(), Long.MaxValue)
      }
      writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      val result = readJournalDao
        .getMessagesAsJournalRow(PersistenceId(pid), SequenceNumber(1), SequenceNumber(1000), Long.MaxValue).runWith(
          Sink.seq
        ).futureValue
      result.map(v => (v.persistenceId, v.sequenceNumber, v.deleted)) should contain theSameElementsAs journalRows.map(
        v => (v.persistenceId, v.sequenceNumber, v.deleted)
      )
    }
  }

  before { createTable }

  after { deleteTable }

}
