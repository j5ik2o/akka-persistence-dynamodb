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

abstract class CurrentEventsByTagTest2(config: Config) extends QueryJournalSpec(config) {

  it should "find all events by tag" in {
    withTestActors() { (actor1, actor2, actor3) =>
      (for {
        _ <- sendMessage(withTags("a", "number"), actor1)
        _ <- sendMessage(withTags("b", "number"), actor2)
        _ <- sendMessage(withTags("c", "number"), actor3)
      } yield ()).toTry should be a 'success

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
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentEventsByTagTest2
    extends CurrentEventsByTagTest2(
      ConfigFactory
        .parseString(
          s"""
             |dynamo-db-journal.dynamodb-client {
             |  endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTest2.dynamoDBPort}/"
             |}
             |
             |dynamo-db-snapshot.dynamodb-client {
             |  endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTest2.dynamoDBPort}/"
             |}
             |
             |dynamo-db-read-journal.dynamodb-client {
             |  endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTest2.dynamoDBPort}/"
             |}
           """.stripMargin
        ).withFallback(ConfigFactory.load())
    )
    with DynamoDBSpecSupport {

  override implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBCurrentEventsByTagTest2.dynamoDBPort

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
