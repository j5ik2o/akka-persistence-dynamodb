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
import com.github.j5ik2o.akka.persistence.dynamodb.query.QueryJournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.typesafe.config.{ Config, ConfigFactory }
import org.testcontainers.DockerClientFactory

import scala.concurrent.duration._

abstract class CurrentEventsByTagTest2(config: Config) extends QueryJournalSpec(config) {

  it should "find all events by tag" in {
    withTestActors() { (actor1, actor2, actor3) =>
      (for {
        _ <- sendMessage(withTags("a", "number"), actor1)
        _ <- sendMessage(withTags("b", "number"), actor2)
        _ <- sendMessage(withTags("c", "number"), actor3)
      } yield ()).toTry should be a Symbol("success")

      withCurrentEventsByTag()("number", 0) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNextPF { case EventEnvelope(Sequence(1), "my-1", 1, "a-1") => }
        tp.expectNextPF { case EventEnvelope(Sequence(2), "my-2", 1, "b-1") => }
        tp.expectNextPF { case EventEnvelope(Sequence(3), "my-3", 1, "c-1") => }
        tp.expectComplete()
      }

      withCurrentEventsByTag()("number", 1) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNextPF { case EventEnvelope(Sequence(2), "my-2", 1, "b-1") => }
        tp.expectNextPF { case EventEnvelope(Sequence(3), "my-3", 1, "c-1") => }
        tp.expectComplete()
      }

      withCurrentEventsByTag()("number", 2) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNextPF { case EventEnvelope(Sequence(3), "my-3", 1, "c-1") => }
        tp.expectComplete()
      }

      withCurrentEventsByTag()("number", 3) { tp =>
        tp.request(Int.MaxValue)
        tp.expectComplete()
      }

      withCurrentEventsByTag()("number", 4) { tp =>
        tp.request(Int.MaxValue)
        tp.expectComplete()
      }
    }
  }
}

object DynamoDBCurrentEventsByTagTest2 {
  val dynamoDBHost: String = DockerClientFactory.instance().dockerHostIpAddress()
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentEventsByTagTest2
    extends CurrentEventsByTagTest2(
      ConfigFactory
        .parseString(
          s"""
             |j5ik2o.dynamo-db-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBCurrentEventsByTagTest2.dynamoDBHost}:${DynamoDBCurrentEventsByTagTest2.dynamoDBPort}/"
             |}
             |
             |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
             |  endpoint = "http://${DynamoDBCurrentEventsByTagTest2.dynamoDBHost}:${DynamoDBCurrentEventsByTagTest2.dynamoDBPort}/"
             |}
             |j5ik2o.dynamo-db-read-journal {
             |  batch-size = 1
             |}
             |j5ik2o.dynamo-db-read-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBCurrentEventsByTagTest2.dynamoDBHost}:${DynamoDBCurrentEventsByTagTest2.dynamoDBPort}/"
             |}
           """.stripMargin
        ).withFallback(ConfigFactory.load("query-reference"))
    )
    with DynamoDBSpecSupport {

  override implicit val pc: PatienceConfig = PatienceConfig(30.seconds, 1.seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBCurrentEventsByTagTest2.dynamoDBPort

  before { createTable() }

  after { deleteTable() }

}
