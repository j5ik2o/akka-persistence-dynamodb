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

import akka.persistence.query.{ EventEnvelope, Sequence }
import com.github.dockerjava.core.DockerClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.query.QueryJournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.dockerController.DockerClientConfigUtil
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._

abstract class CurrentEventsByTagTestDeletedEventsTest(config: Config) extends QueryJournalSpec(config) {
  it should "show deleted events in event stream" in {
    withTestActors() { (actor1, _, _) =>
      sendMessage(withTags("a", "one"), actor1).toTry should be a Symbol("success")
      sendMessage(withTags("b", "one"), actor1).toTry should be a Symbol("success")
      sendMessage(withTags("c", "one"), actor1).toTry should be a Symbol("success")

      deleteEvents(actor1, 0).toTry should be a Symbol("success")

      withCurrentEventsByTag()("one", 0) { tp =>
        tp.request(Int.MaxValue)
          .expectNext(new EventEnvelope(Sequence(1), "my-1", 1, "a-1", 0L))
          .expectNext(new EventEnvelope(Sequence(2), "my-1", 2, "b-2", 0L))
          .expectNext(new EventEnvelope(Sequence(3), "my-1", 3, "c-3", 0L))
          .expectComplete()
      }

      deleteEvents(actor1, 1).toTry should be a Symbol("success")

      withCurrentEventsByTag()("one", 0) { tp =>
        tp.request(Int.MaxValue)
          .expectNext(new EventEnvelope(Sequence(1), "my-1", 1, "a-1", 0L))
          .expectNext(new EventEnvelope(Sequence(2), "my-1", 2, "b-2", 0L))
          .expectNext(new EventEnvelope(Sequence(3), "my-1", 3, "c-3", 0L))
          .expectComplete()
      }

      deleteEvents(actor1, 2).toTry should be a Symbol("success")

      withCurrentEventsByTag()("one", 0) { tp =>
        tp.request(Int.MaxValue)
          .expectNext(new EventEnvelope(Sequence(1), "my-1", 1, "a-1", 0L))
          .expectNext(new EventEnvelope(Sequence(2), "my-1", 2, "b-2", 0L))
          .expectNext(new EventEnvelope(Sequence(3), "my-1", 3, "c-3", 0L))
          .expectComplete()
      }

      deleteEvents(actor1, 3).toTry should be a Symbol("success")

      withCurrentEventsByTag()("one", 0) { tp =>
        tp.request(Int.MaxValue)
          .expectNext(new EventEnvelope(Sequence(1), "my-1", 1, "a-1", 0L))
          .expectNext(new EventEnvelope(Sequence(2), "my-1", 2, "b-2", 0L))
          .expectNext(new EventEnvelope(Sequence(3), "my-1", 3, "c-3", 0L))
          .expectComplete()
      }
    }
  }
}

object DynamoDBCurrentEventsByTagTestDeletedEventsTest {
  val dockerClientConfig: DockerClientConfig = DockerClientConfigUtil.buildConfigAwareOfDockerMachine()
  val dynamoDBHost: String                   = DockerClientConfigUtil.dockerHost(dockerClientConfig)
  val dynamoDBPort: Int                      = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentEventsByTagTestDeletedEventsTest
    extends CurrentEventsByTagTestDeletedEventsTest(
      ConfigFactory
        .parseString(
          s"""
           |j5ik2o.dynamo-db-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBHost}:${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBPort}/"
           |  }
           |}
           |
           |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
           |  endpoint = "http://${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBHost}:${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBPort}/"
           |}
           |
           |j5ik2o.dynamo-db-read-journal { 
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBHost}:${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBPort}/"
           |  }
           |}
         """.stripMargin
        ).withFallback(ConfigFactory.load("query-reference"))
    )
    with DynamoDBSpecSupport {

  private val testTimeFactor: Double = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toDouble

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled((30 * testTimeFactor).seconds), interval = scaled((1 * testTimeFactor).seconds))

  override protected lazy val dynamoDBHost: String = DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBHost
  override protected lazy val dynamoDBPort: Int    = DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBPort

  override protected def afterStartContainers(): Unit = {
    createTable()
  }

  override protected def beforeStopContainers(): Unit = {
    deleteTable()
  }

}
