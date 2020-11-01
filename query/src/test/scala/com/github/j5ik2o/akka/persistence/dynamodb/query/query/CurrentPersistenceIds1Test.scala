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

import java.net.URI

import akka.pattern.ask
import akka.persistence.query.{ EventEnvelope, Sequence }
import com.github.j5ik2o.akka.persistence.dynamodb.query.QueryJournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.{ Config, ConfigFactory }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.Future
import scala.concurrent.duration._

abstract class CurrentPersistenceIds1Test(config: Config) extends QueryJournalSpec(config) {

  it should "not find any events for unknown pid" in
  withCurrentEventsByPersistenceId()("unkown-pid", 0L, Long.MaxValue) { tp =>
    tp.request(Int.MaxValue)
    tp.expectComplete()
  }

  it should "find events from an offset" in {
    withTestActors() { (actor1, actor2, actor3) =>
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
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentPersistenceIds1Test
    extends CurrentPersistenceIds1Test(
      ConfigFactory
        .parseString(
          s"""
           |j5ik2o.dynamo-db-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://127.0.0.1:${DynamoDBCurrentPersistenceIds1Test.dynamoDBPort}/"
           |  }
           |}
           |
           |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
           |  endpoint = "http://127.0.0.1:${DynamoDBCurrentPersistenceIds1Test.dynamoDBPort}/"
           |}
           |
           |j5ik2o.dynamo-db-read-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://127.0.0.1:${DynamoDBCurrentPersistenceIds1Test.dynamoDBPort}/"
           |  }
           |}
           """.stripMargin
        ).withFallback(ConfigFactory.load("query-reference"))
    )
    with DynamoDBSpecSupport {

  override implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBCurrentPersistenceIds1Test.dynamoDBPort

  before { createTable }

  after { deleteTable }

}
