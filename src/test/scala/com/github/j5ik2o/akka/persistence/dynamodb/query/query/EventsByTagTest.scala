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
import com.github.j5ik2o.akka.persistence.dynamodb.query.TestSpec
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.duration._

abstract class EventsByTagTest(config: String) extends TestSpec(config) {

  it should "find events by tag from an offset using Offset interface " in {
    withTestActors() { (actor1, actor2, actor3) =>
      actor1 ! withTags(1, "number3")
      actor2 ! withTags(2, "number3")
      actor3 ! withTags(3, "number3")

      eventually {
        countJournal shouldBe 3
      }

      currentEventsByTagAsList("number3", Sequence(0)) should matchPattern {
        case List(EventEnvelope(Sequence(1), _, _, _),
                  EventEnvelope(Sequence(2), _, _, _),
                  EventEnvelope(Sequence(3), _, _, _)) =>
      }

      currentEventsByTagAsList("number3", Sequence(1)) should matchPattern {
        case List(EventEnvelope(Sequence(2), _, _, _), EventEnvelope(Sequence(3), _, _, _)) =>
      }

      currentEventsByTagAsList("number3", Sequence(2)) should matchPattern {
        case List(EventEnvelope(Sequence(3), _, _, _)) =>
      }

      currentEventsByTagAsList("number3", Sequence(3)) should matchPattern {
        case Nil =>
      }

      currentEventsByTagAsList("number3", Sequence(4)) should matchPattern {
        case Nil =>
      }

      withEventsByTag()("number3", Sequence(0)) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNextPF { case EventEnvelope(Sequence(1), _, _, _) => }
        tp.expectNextPF { case EventEnvelope(Sequence(2), _, _, _) => }
        tp.expectNextPF { case EventEnvelope(Sequence(3), _, _, _) => }
        tp.expectNoMessage(100.millis)
        tp.cancel()
      }

      withEventsByTag()("number3", Sequence(1)) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNextPF { case EventEnvelope(Sequence(2), _, _, _) => }
        tp.expectNextPF { case EventEnvelope(Sequence(3), _, _, _) => }
        tp.expectNoMessage(100.millis)
        tp.cancel()
      }

      withEventsByTag()("number3", Sequence(2)) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNextPF { case EventEnvelope(Sequence(3), _, _, _) => }
        tp.expectNoMessage(100.millis)
        tp.cancel()
      }

      withEventsByTag()("number3", Sequence(3)) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNoMessage(100.millis)
        tp.cancel()
      }

      withEventsByTag()("number3", Sequence(4)) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNoMessage(100.millis)
        tp.cancel()
      }

      withEventsByTag()("number3", Sequence(4)) { tp =>
        tp.request(Int.MaxValue)
        tp.expectNoMessage(100.millis)

        // new event
        actor1 ! withTags(4, "number3")
        tp.cancel()
      }
    }
  }
}

class DynamoDBEventsByTagTest extends EventsByTagTest("default.conf") with DynamoDBSpecSupport {

  override implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = 8000

  val underlying: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .httpClient(NettyNioAsyncHttpClient.builder().maxConcurrency(1).build())
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def asyncClient: DynamoDBAsyncClientV2 = DynamoDBAsyncClientV2(underlying)

  override def afterAll(): Unit = {
    underlying.close()
    super.afterAll()
  }

  before { createTable }

  after { deleteTable }

}
