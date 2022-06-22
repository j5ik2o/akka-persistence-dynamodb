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
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.testcontainers.DockerClientFactory

import scala.concurrent.duration._

object DynamoDBSnapshotStoreV1AsyncSpec {
  val dynamoDBHost: String = DockerClientFactory.instance().dockerHostIpAddress()
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
}

final class DynamoDBSnapshotStoreV1AsyncSpec
    extends SnapshotStoreSpec(
      ConfigFactory
        .parseString(
          s"""
             |j5ik2o.dynamo-db-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV1AsyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV1AsyncSpec.dynamoDBPort}/"
             |}
             |
             |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV1AsyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV1AsyncSpec.dynamoDBPort}/"
             |  region = "ap-northeast-1"
             |  access-key-id = "x"
             |  secret-key = "x"
             |  client-version = "v1"
             |  client-type = "async"
             |}
             |
             |j5ik2o.dynamo-db-read-journal.dynamo-db-client {
             |  endpoint = "http://${DynamoDBSnapshotStoreV1AsyncSpec.dynamoDBHost}:${DynamoDBSnapshotStoreV1AsyncSpec.dynamoDBPort}/"
             |}
         """.stripMargin
        ).withFallback(ConfigFactory.load("snapshot-reference"))
    )
    with ScalaFutures
    with DynamoDBSpecSupport {

  implicit val pc: PatienceConfig = PatienceConfig(30.seconds, 1.seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBSnapshotStoreV1AsyncSpec.dynamoDBPort

  before { createTable() }

  after { deleteTable() }

}
