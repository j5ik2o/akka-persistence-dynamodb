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

import com.github.dockerjava.core.DockerClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.query.QueryJournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.dockerController.DockerClientConfigUtil
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._

abstract class CurrentEventsByTagTest1(config: Config) extends QueryJournalSpec(config) {

  it should "not find an event by tag for unknown tag" in {
    withTestActors() { (actor1, actor2, actor3) =>
      List(
        sendMessage(withTags("a", "one"), actor1),
        sendMessage(withTags("a", "two"), actor2),
        sendMessage(withTags("a", "three"), actor3)
      ).toTry should be a Symbol("success")

      withCurrentEventsByTag()("unknown", 0) { tp =>
        tp.request(Int.MaxValue)
        tp.expectComplete()
      }
    }
  }
}

object DynamoDBCurrentEventsByTagTest1 {
  val dockerClientConfig: DockerClientConfig = DockerClientConfigUtil.buildConfigAwareOfDockerMachine()
  val dynamoDBHost: String                   = DockerClientConfigUtil.dockerHost(dockerClientConfig)
  val dynamoDBPort: Int                      = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentEventsByTagTest1
    extends CurrentEventsByTagTest1(
      ConfigFactory
        .parseString(
          s"""
           |j5ik2o.dynamo-db-journal.dynamo-db-client {
           |  endpoint = "http://${DynamoDBCurrentEventsByTagTest1.dynamoDBHost}:${DynamoDBCurrentEventsByTagTest1.dynamoDBPort}/"
           |}
           |
           |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
           |  endpoint = "http://${DynamoDBCurrentEventsByTagTest1.dynamoDBHost}:${DynamoDBCurrentEventsByTagTest1.dynamoDBPort}/"
           |}
           |j5ik2o.dynamo-db-read-journal {
           |  batch-size = 1
           |}
           |j5ik2o.dynamo-db-read-journal.dynamo-db-client {
           |  endpoint = "http://${DynamoDBCurrentEventsByTagTest1.dynamoDBHost}:${DynamoDBCurrentEventsByTagTest1.dynamoDBPort}/"
           |}
           """.stripMargin
        ).withFallback(ConfigFactory.load("query-reference"))
    )
    with DynamoDBSpecSupport {

  private val testTimeFactor: Double = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toDouble

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled((30 * testTimeFactor).seconds), interval = scaled((1 * testTimeFactor).seconds))

  override protected lazy val dynamoDBHost: String =
    DynamoDBCurrentEventsByTagTest1.dynamoDBHost

  override protected lazy val dynamoDBPort: Int = DynamoDBCurrentEventsByTagTest1.dynamoDBPort

  override protected def afterStartContainers(): Unit = {
    createTable()
  }

  override protected def beforeStopContainers(): Unit = {
    deleteTable()
  }

}
