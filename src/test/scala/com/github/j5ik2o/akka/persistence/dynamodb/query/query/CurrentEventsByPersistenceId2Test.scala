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

abstract class CurrentEventsByPersistenceId2Test(config: Config) extends QueryJournalSpec(config) {

  it should "find events from an offset" in {
    withTestActors() { (actor1, actor2, actor3) =>
      List.fill(7)(sendMessage("a", actor1)).toTry should be a 'success

      withCurrentEventsByPersistenceId()("my-1", 0, 0) { tp =>
        tp.request(Int.MaxValue)
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 1, 0) { tp =>
        tp.request(Int.MaxValue)
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 0, 1) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 1, 1) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 1, 2) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        tp.expectNext(EventEnvelope(Sequence(2), "my-1", 2, "a-2"))
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", -1, 2) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        tp.expectNext(EventEnvelope(Sequence(2), "my-1", 2, "a-2"))
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 2, 3) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNext(EventEnvelope(Sequence(2), "my-1", 2, "a-2"))
        tp.expectNext(EventEnvelope(Sequence(3), "my-1", 3, "a-3"))
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 6, 7) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNext(EventEnvelope(Sequence(6), "my-1", 6, "a-6"))
        tp.expectNext(EventEnvelope(Sequence(7), "my-1", 7, "a-7"))
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 7, 7) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNext(EventEnvelope(Sequence(7), "my-1", 7, "a-7"))
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 8) { tp =>
        tp.request(Int.MaxValue)
        tp.expectComplete()
      }

      withCurrentEventsByPersistenceId()("my-1", 9, 0) { tp =>
        tp.request(Int.MaxValue)
        tp.expectComplete()
      }
    }
  }
}

object DynamoDBCurrentEventsByPersistenceId2Test {
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentEventsByPersistenceId2Test
    extends CurrentEventsByPersistenceId2Test(
      ConfigFactory
        .parseString(
          s"""
           |dynamo-db-journal.dynamodb-client {
           |  endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByPersistenceId2Test.dynamoDBPort}/"
           |}
           |
           |dynamo-db-snapshot.dynamodb-client {
           |  endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByPersistenceId2Test.dynamoDBPort}/"
           |}
           |
           |dynamo-db-read-journal.dynamodb-client {
           |  endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByPersistenceId2Test.dynamoDBPort}/"
           |}
      """.stripMargin
        ).withFallback(ConfigFactory.load())
    )
    with DynamoDBSpecSupport {

  override protected lazy val dynamoDBPort: Int = DynamoDBCurrentEventsByPersistenceId2Test.dynamoDBPort

  val underlying: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def asyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(underlying)

  override def afterAll(): Unit = {
    underlying.close()
    super.afterAll()
  }

  before { createTable }

  after { deleteTable }
}
