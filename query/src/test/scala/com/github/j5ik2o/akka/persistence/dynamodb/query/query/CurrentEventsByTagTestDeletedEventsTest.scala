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

import akka.persistence.query.{ EventEnvelope, Sequence }
import com.github.j5ik2o.akka.persistence.dynamodb.query.QueryJournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.{ Config, ConfigFactory }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

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
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentEventsByTagTestDeletedEventsTest
    extends CurrentEventsByTagTestDeletedEventsTest(
      ConfigFactory
        .parseString(
          s"""
           |j5ik2o.dynamo-db-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBPort}/"
           |  }
           |}
           |
           |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
           |  endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBPort}/"
           |}
           |
           |j5ik2o.dynamo-db-read-journal { 
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBPort}/"
           |  }
           |}
         """.stripMargin
        ).withFallback(ConfigFactory.load("query-reference"))
    )
    with DynamoDBSpecSupport {

  override implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBCurrentEventsByTagTestDeletedEventsTest.dynamoDBPort

  val underlying: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def dynamoDbAsyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(underlying)

  override def afterAll(): Unit = {
    underlying.close()
    super.afterAll()
  }

  before { createTable }

  after { deleteTable }

}
