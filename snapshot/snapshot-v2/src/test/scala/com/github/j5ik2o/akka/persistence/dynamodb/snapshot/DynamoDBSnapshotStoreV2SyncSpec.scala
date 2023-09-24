/*
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
package com.github.j5ik2o.akka.persistence.dynamodb.snapshot

import akka.persistence.snapshot.SnapshotStoreSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBContainerHelper, RandomPortUtil }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

object DynamoDBSnapshotStoreV2SyncSpec {
  val dynamoDBHost: String = "localhost"
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
}

final class DynamoDBSnapshotStoreV2SyncSpec
    extends SnapshotStoreSpec(
      ConfigFactory
        .parseString(
          s"""
             |j5ik2o.dynamo-db-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV2SyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV2SyncSpec.dynamoDBPort}/"
             |}
             |
             |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV2SyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV2SyncSpec.dynamoDBPort}/"
             |  region = "ap-northeast-1"
             |  access-key-id = "x"
             |  secret-key = "x"
             |  client-version = "v2"
             |  client-type = "sync"
             |}
             |
             |j5ik2o.dynamo-db-read-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV2SyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV2SyncSpec.dynamoDBPort}/"
             |}
         """.stripMargin
        ).withFallback(ConfigFactory.load("snapshot-reference"))
    )
    with ScalaFutures
    with DynamoDBContainerHelper {

  implicit val pc: PatienceConfig = PatienceConfig(30.seconds, 1.seconds)

  override lazy val dynamoDBPort: Int = DynamoDBSnapshotStoreV2SyncSpec.dynamoDBPort

  override def afterStartContainers(): Unit = {
    super.afterStartContainers()
    createTable()
  }

  override def beforeStopContainers(): Unit = {
    deleteTable()
    super.beforeStopContainers()
  }

}
