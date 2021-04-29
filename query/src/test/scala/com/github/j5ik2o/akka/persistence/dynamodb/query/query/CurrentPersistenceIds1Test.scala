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

import akka.pattern.ask
import akka.persistence.query.{ EventEnvelope, Sequence }
import com.github.dockerjava.core.DockerClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.query.QueryJournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.dockerController.DockerClientConfigUtil
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.Future
import scala.concurrent.duration._

abstract class CurrentPersistenceIds1Test(config: Config) extends QueryJournalSpec(config) {

  it should "not find any events for unknown pid" in
  withCurrentEventsByPersistenceId()("unkown-pid", 0L, Long.MaxValue) { tp =>
    tp.request(Int.MaxValue)
    tp.expectComplete()
  }

  it should "find events from an offset" in {
    withTestActors() { (actor1, _, _) =>
      Future.sequence(Range.inclusive(1, 4).map(_ => actor1 ? "a")).toTry should be a Symbol("success")

      withCurrentEventsByPersistenceId()("my-1", 2, 3) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNext(new EventEnvelope(Sequence(2), "my-1", 2, "a-2", 0L))
        tp.expectNext(new EventEnvelope(Sequence(3), "my-1", 3, "a-3", 0L))
        tp.expectComplete()
      }
    }
  }
}

object DynamoDBCurrentPersistenceIds1Test {
  val dockerClientConfig: DockerClientConfig = DockerClientConfigUtil.buildConfigAwareOfDockerMachine()
  val dynamoDBHost: String                   = DockerClientConfigUtil.dockerHost(dockerClientConfig)
  val dynamoDBPort: Int                      = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentPersistenceIds1Test
    extends CurrentPersistenceIds1Test(
      ConfigFactory
        .parseString(
          s"""
           |j5ik2o.dynamo-db-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://${DynamoDBCurrentPersistenceIds1Test.dynamoDBHost}:${DynamoDBCurrentPersistenceIds1Test.dynamoDBPort}/"
           |  }
           |}
           |
           |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
           |  endpoint = "http://${DynamoDBCurrentPersistenceIds1Test.dynamoDBHost}:${DynamoDBCurrentPersistenceIds1Test.dynamoDBPort}/"
           |}
           |
           |j5ik2o.dynamo-db-read-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://${DynamoDBCurrentPersistenceIds1Test.dynamoDBHost}:${DynamoDBCurrentPersistenceIds1Test.dynamoDBPort}/"
           |  }
           |}
           """.stripMargin
        ).withFallback(ConfigFactory.load("query-reference"))
    )
    with DynamoDBSpecSupport {

  private val testTimeFactor: Double = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toDouble

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled((30 * testTimeFactor).seconds), interval = scaled((1 * testTimeFactor).seconds))

  override protected lazy val dynamoDBHost: String = DynamoDBCurrentPersistenceIds1Test.dynamoDBHost
  override protected lazy val dynamoDBPort: Int    = DynamoDBCurrentPersistenceIds1Test.dynamoDBPort

  override protected def afterStartContainers(): Unit = {
    createTable()
  }

  override protected def beforeStopContainers(): Unit = {
    deleteTable()
  }
}
