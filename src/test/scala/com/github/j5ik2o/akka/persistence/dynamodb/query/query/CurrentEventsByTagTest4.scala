/*
 * Copyright 2017 Dennis Vriend
 * Copyright 2019 Junichi Katos
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

import scala.concurrent.duration._

abstract class CurrentEventsByTagTest4(config: Config) extends QueryJournalSpec(config) {

  it should "persist and find a tagged event with one tag" in
  withTestActors() { (actor1, actor2, actor3) =>
    (actor1 ? withTags(1, "one2")).toTry should be a 'success

    withClue("query should find the event by tag") {
      withCurrentEventsByTag()("one2", 0) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNextPF { case EventEnvelope(Sequence(1), _, _, _) => }
        tp.expectComplete()
      }
    }

    withClue("query should find the event by persistenceId") {
      withCurrentEventsByPersistenceId()("my-1", 1, 1) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNextPF { case EventEnvelope(Sequence(1), _, _, _) => }
        tp.expectComplete()
      }
    }
  }
}

object DynamoDBCurrentEventsByTagTest4 {
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class DynamoDBCurrentEventsByTagTest4
    extends CurrentEventsByTagTest4(
      ConfigFactory
        .parseString(
          s"""
             |dynamo-db-journal{
             |  query-batch-size = 1
             |  dynamodb-client {
             |    endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTest4.dynamoDBPort}/"
             |  }
             |}
             |
             |dynamo-db-snapshot.dynamodb-client {
             |  endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTest4.dynamoDBPort}/"
             |}
             |
             |dynamo-db-read-journal {
             |  query-batch-size = 1
             |  dynamodb-client {
             |    endpoint = "http://127.0.0.1:${DynamoDBCurrentEventsByTagTest4.dynamoDBPort}/"
             |  }
             |}
           """.stripMargin
        ).withFallback(ConfigFactory.load())
    )
    with DynamoDBSpecSupport {

  override implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBCurrentEventsByTagTest4.dynamoDBPort

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
