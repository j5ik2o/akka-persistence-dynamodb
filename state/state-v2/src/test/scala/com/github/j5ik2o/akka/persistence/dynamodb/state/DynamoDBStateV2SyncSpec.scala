/*
 * Copyright 2022 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.persistence.state.DurableStateStoreRegistry
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.{ DynamoDBDurableStateStoreV2, StateSpecBase }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBContainerHelper, RandomPortUtil }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.util.UUID

object DynamoDBStateV2SyncSpec {
  val dynamoDBHost: String = "localhost"
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
}

final class DynamoDBStateV2SyncSpec
    extends StateSpecBase(
      ConfigHelper
        .config(
          Some("state-reference"),
          legacyConfigFormat = false,
          legacyJournalMode = false,
          dynamoDBHost = DynamoDBStateV2SyncSpec.dynamoDBHost,
          dynamoDBPort = DynamoDBStateV2SyncSpec.dynamoDBPort,
          clientVersion = ClientVersion.V2.toString,
          clientType = ClientType.Sync.toString
        )
    )
    with ScalaFutures
    with DynamoDBContainerHelper {

  implicit val pc: PatienceConfig = PatienceConfig(30.seconds, 1.seconds)

  override lazy val dynamoDBPort: Int = DynamoDBStateV2SyncSpec.dynamoDBPort

  "A durable state store plugin" - {
    "instantiate a DynamoDBDurableDataStore successfully" in {
      val store = DurableStateStoreRegistry
        .get(system)
        .durableStateStoreFor[DynamoDBDurableStateStoreV2[String]](DynamoDBDurableStateStoreProvider.Identifier)

      {
        val id       = UUID.randomUUID().toString
        val revision = 1
        val data     = "abc"
        val tag      = ""
        store.upsertObject(id, revision, data, tag).futureValue
        val result = store.getObject(id).futureValue
        result.value shouldBe Some(data)
      }
      {
        val id       = UUID.randomUUID().toString
        val revision = 1
        val data     = "def"
        val tag      = UUID.randomUUID().toString
        store.upsertObject(id, revision, data, tag).futureValue
        val result = store.getRawObject(id).futureValue
        result match {
          case just: GetRawObjectResult.Just[String] =>
            just.value shouldBe data
            just.tag shouldBe Some(tag)
          case _ =>
            fail()
        }
      }

      store shouldBe a[DynamoDBDurableStateStoreV2[_]]
      store.system.settings.config shouldBe system.settings.config
    }
  }

  override def afterStartContainers(): Unit = {
    super.afterStartContainers()
    createTable()
  }

  override def beforeStopContainers(): Unit = {
    deleteTable()
    super.beforeStopContainers()
  }

}
