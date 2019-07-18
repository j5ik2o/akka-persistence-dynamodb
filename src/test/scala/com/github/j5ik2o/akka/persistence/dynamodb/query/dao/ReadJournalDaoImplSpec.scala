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
import com.github.j5ik2o.akka.persistence.dynamodb.jmx.MetricsFunctions
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamoDBSpecSupport
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.monix.DynamoDbMonixClient
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FreeSpecLike, Matchers }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.duration._

class ReadJournalDaoImplSpec
    extends TestKit(ActorSystem("ReadJournalDaoImplSpec", ConfigFactory.load("default.conf")))
    with FreeSpecLike
    with Matchers
    with ScalaFutures
    with DynamoDBSpecSupport {

  implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  val underlyingAsync: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.WriteJournalDaoImpl

  val asyncClient  = DynamoDbAsyncClient(underlyingAsync)
  val taskClient   = DynamoDbMonixClient(asyncClient)
  val streamClient = DynamoDbAkkaClient(asyncClient)

  private val serialization = SerializationExtension(system)

  protected val journalPluginConfig: JournalPluginConfig =
    JournalPluginConfig.fromConfig(system.settings.config.asConfig("dynamodb-journal"))

  protected val queryPluginConfig: QueryPluginConfig =
    QueryPluginConfig.fromConfig(system.settings.config.asConfig("dynamo-db-read-journal"))

  implicit val mat = ActorMaterializer()
  implicit val ec  = system.dispatcher

  val readJournalDao = new ReadJournalDaoImpl(asyncClient, serialization, queryPluginConfig, MetricsFunctions())(ec)

  val writeJournalDao =
    new WriteJournalDaoImpl(asyncClient, serialization, journalPluginConfig, MetricsFunctions())(ec, mat)

  val sch = Scheduler(ec)

  "ReadJournalDaoImpl" - {
    "allPersistenceIdsSource" in {
      val journalRows = (1 to 100).map { n =>
        JournalRow(PersistenceId(n.toString), SequenceNumber(1), deleted = false, "ABC".getBytes(), Long.MaxValue)
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
        JournalRow(PersistenceId(pid), SequenceNumber(n), deleted = false, "ABC".getBytes(), Long.MaxValue)
      }
      writeJournalDao.putMessages(journalRows).runWith(Sink.head).futureValue
      val result = readJournalDao
        .getMessages(PersistenceId(pid), SequenceNumber(1), SequenceNumber(1000), Long.MaxValue).runWith(Sink.seq).futureValue
      result.map(v => (v.persistenceId, v.sequenceNumber, v.deleted)) should contain theSameElementsAs journalRows.map(
        v => (v.persistenceId, v.sequenceNumber, v.deleted)
      )
    }
  }

  before { createTable }

  after { deleteTable }

}
