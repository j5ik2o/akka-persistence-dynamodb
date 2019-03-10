/*
 * Copyright 2017 Dennis Vriend
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
import com.github.j5ik2o.akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.duration._

abstract class EventsByPersistenceIdTest(config: String) extends TestSpec(config) {

  it should "complete when toSeqNr=0" in
  withEventsByPersistenceId()("unkown-pid", 0L, 0L) { tp =>
    tp.request(Int.MaxValue)
    if (readJournal.isInstanceOf[DynamoDBReadJournal]) {
      tp.expectComplete()
    } else {
      tp.expectNoMessage(300.millis)
    }
    tp.cancel
  }

  it should "not complete when toSeqNr = Long.MaxValue" in
  withEventsByPersistenceId()("unkown-pid", 0L, Long.MaxValue) { tp =>
    tp.request(Int.MaxValue)
    tp.expectNoMessage(300.millis)
    tp.cancel
  }

  it should "complete when toSeqNr is reached" in withTestActors() { (actor1, actor2, actor3) =>
    actor1 ! "a"
    actor1 ! "a"
    actor1 ! "a"

    eventually {
      countJournal shouldBe 3
    }

    withEventsByPersistenceId()("my-1", 0, 1) { tp =>
      tp.request(Int.MaxValue)
        .expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        .expectComplete()
    }

    withEventsByPersistenceId()("my-1", 1, 1) { tp =>
      tp.request(Int.MaxValue)
        .expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        .expectComplete()
    }

    withEventsByPersistenceId()("my-1", 1, 2) { tp =>
      tp.request(Int.MaxValue)
        .expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        .expectNext(EventEnvelope(Sequence(2), "my-1", 2, "a-2"))
        .expectComplete()
    }

    withEventsByPersistenceId()("my-1", 2, 2) { tp =>
      tp.request(Int.MaxValue)
        .expectNext(EventEnvelope(Sequence(2), "my-1", 2, "a-2"))
        .expectComplete()
    }

    withEventsByPersistenceId()("my-1", 2, 3) { tp =>
      tp.request(Int.MaxValue)
        .expectNext(EventEnvelope(Sequence(2), "my-1", 2, "a-2"))
        .expectNext(EventEnvelope(Sequence(3), "my-1", 3, "a-3"))
        .expectComplete()
    }

    withEventsByPersistenceId()("my-1", 3, 3) { tp =>
      tp.request(Int.MaxValue)
        .expectNext(EventEnvelope(Sequence(3), "my-1", 3, "a-3"))
        .expectComplete()
    }

    withEventsByPersistenceId()("my-1", 0, 3) { tp =>
      tp.request(Int.MaxValue)
        .expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        .expectNext(EventEnvelope(Sequence(2), "my-1", 2, "a-2"))
        .expectNext(EventEnvelope(Sequence(3), "my-1", 3, "a-3"))
        .expectComplete()
    }

    withEventsByPersistenceId()("my-1", 1, 3) { tp =>
      tp.request(Int.MaxValue)
        .expectNext(EventEnvelope(Sequence(1), "my-1", 1, "a-1"))
        .expectNext(EventEnvelope(Sequence(2), "my-1", 2, "a-2"))
        .expectNext(EventEnvelope(Sequence(3), "my-1", 3, "a-3"))
        .expectComplete()
    }
  }
}
//
//class LevelDbEventsByPersistenceIdTest extends EventsByPersistenceIdTest("leveldb.conf")
//
//class InMemoryEventsByPersistenceIdTest extends EventsByPersistenceIdTest("inmemory.conf")

class DynamoDBEventsByPersistenceIdTest extends EventsByPersistenceIdTest("default.conf") with DynamoDBSpecSupport {

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
