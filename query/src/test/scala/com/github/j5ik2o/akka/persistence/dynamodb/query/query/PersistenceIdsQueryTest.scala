/*
 * Copyright 2017 Dennis Vriend
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

package com.github.j5ik2o.akka.persistence.dynamodb.query.query

import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import com.github.j5ik2o.akka.persistence.dynamodb.query.QueryJournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.typesafe.config.{ Config, ConfigFactory }
import org.testcontainers.DockerClientFactory

import scala.concurrent.duration._

abstract class PersistenceIdsQueryTest(config: Config) extends QueryJournalSpec(config) {

  it should "not terminate the stream when there are not pids" in
  withPersistenceIds() { tp =>
    tp.request(Long.MaxValue)
    tp.expectNoMessage(100.millis)
    tp.cancel()
    tp.expectNoMessage(100.millis)
  }

  it should "find persistenceIds for actors" in
  withTestActors() { (actor1, actor2, actor3) =>
    withPersistenceIds() { tp =>
      tp.request(Int.MaxValue)

      countJournal shouldBe 0 // note, there are *no* events

      if (readJournal.isInstanceOf[LeveldbReadJournal]) {
        // curious, empty event store but the
        // read-journal knows about the persistent actors
        tp.expectNextPF {
          case "my-1" =>
          case "my-2" =>
          case "my-3" =>
        }
        tp.expectNextPF {
          case "my-1" =>
          case "my-2" =>
          case "my-3" =>
        }
        tp.expectNextPF {
          case "my-1" =>
          case "my-2" =>
          case "my-3" =>
        }
      }

      tp.expectNoMessage(100.millis)
      countJournal shouldBe 0 // note, there are *no* events

      actor1 ! 1

      eventually {
        countJournal shouldBe 1
      }
      if (readJournal.isInstanceOf[DynamoDBReadJournal]) {
        tp.expectNext("my-1")
      }
      tp.expectNoMessage(1.seconds)

      actor2 ! 1
      eventually {
        countJournal shouldBe 2
      }

      if (readJournal.isInstanceOf[DynamoDBReadJournal]) {
        tp.expectNext("my-2")
      }
      tp.expectNoMessage(100.millis)

      actor3 ! 1
      eventually {
        countJournal shouldBe 3
      }
      if (readJournal.isInstanceOf[DynamoDBReadJournal]) {
        tp.expectNext("my-3")
      }
      tp.expectNoMessage(100.millis)

      actor1 ! 1
      eventually {
        countJournal shouldBe 4
      }

      tp.expectNoMessage(100.millis)

      actor2 ! 1
      eventually {
        countJournal shouldBe 5
      }

      tp.expectNoMessage(100.millis)

      actor3 ! 1
      eventually {
        countJournal shouldBe 6
      }

      tp.expectNoMessage(100.millis)

      tp.cancel()
      tp.expectNoMessage(100.millis)
    }
  }
}

object DynamoDBPersistenceIdsQueryTest {
  val dynamoDBHost: String = DockerClientFactory.instance().dockerHostIpAddress()
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
}

class DynamoDBPersistenceIdsQueryTest
    extends PersistenceIdsQueryTest(
      ConfigFactory
        .parseString(
          s"""
           |j5ik2o.dynamo-db-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://${DynamoDBPersistenceIdsQueryTest.dynamoDBHost}:${DynamoDBPersistenceIdsQueryTest.dynamoDBPort}/"
           |  }
           |}
           |
           |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
           |  endpoint = "http://${DynamoDBPersistenceIdsQueryTest.dynamoDBHost}:${DynamoDBPersistenceIdsQueryTest.dynamoDBPort}/"
           |}
           |
           |j5ik2o.dynamo-db-read-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://${DynamoDBPersistenceIdsQueryTest.dynamoDBHost}:${DynamoDBPersistenceIdsQueryTest.dynamoDBPort}/"
           |  }
           |}
         """.stripMargin
        ).withFallback(ConfigFactory.load("query-reference"))
    )
    with DynamoDBSpecSupport {

  override implicit val pc: PatienceConfig = PatienceConfig(30.seconds, 1.seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBPersistenceIdsQueryTest.dynamoDBPort

  before { createTable() }

  after { deleteTable() }

}
