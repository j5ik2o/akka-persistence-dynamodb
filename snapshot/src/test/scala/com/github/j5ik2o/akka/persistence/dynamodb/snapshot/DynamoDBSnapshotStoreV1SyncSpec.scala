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
import com.github.dockerjava.core.DockerClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.dockerController.DockerClientConfigUtil
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

object DynamoDBSnapshotStoreV1SyncSpec {
  val dockerClientConfig: DockerClientConfig = DockerClientConfigUtil.buildConfigAwareOfDockerMachine()
  val dynamoDBHost: String                   = DockerClientConfigUtil.dockerHost(dockerClientConfig)
  val dynamoDBPort: Int                      = RandomPortUtil.temporaryServerPort()
}

class DynamoDBSnapshotStoreV1SyncSpec
    extends SnapshotStoreSpec(
      ConfigFactory
        .parseString(
          s"""
             |j5ik2o.dynamo-db-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV1SyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV2AsyncSpec.dynamoDBPort}/"
             |}
             |
             |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV1SyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV2AsyncSpec.dynamoDBPort}/"
             |  region = "ap-northeast-1"
             |  access-key-id = "x"
             |  secret-key = "x"
             |  client-version = "v1"
             |  client-type = "sync"
             |}
             |
             |j5ik2o.dynamo-db-read-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV1SyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV2AsyncSpec.dynamoDBPort}/"
             |}
         """.stripMargin
        ).withFallback(ConfigFactory.load("snapshot-reference"))
    )
    with ScalaFutures
    with DynamoDBSpecSupport {

  private val testTimeFactor: Double = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toDouble

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled((30 * testTimeFactor).seconds), interval = scaled((1 * testTimeFactor).seconds))

  override protected lazy val dynamoDBHost: String = DynamoDBSnapshotStoreV2AsyncSpec.dynamoDBHost
  override protected lazy val dynamoDBPort: Int    = DynamoDBSnapshotStoreV2AsyncSpec.dynamoDBPort

  override protected def afterStartContainers(): Unit = {
    createTable()
  }

  override protected def beforeStopContainers(): Unit = {
    deleteTable()
  }

}
