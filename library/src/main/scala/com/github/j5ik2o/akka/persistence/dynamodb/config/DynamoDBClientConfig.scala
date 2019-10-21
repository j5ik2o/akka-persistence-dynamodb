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
package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

import scala.concurrent.duration._

object DynamoDBClientConfig {

  def fromConfig(rootConfig: Config): DynamoDBClientConfig = {
    val result = DynamoDBClientConfig(
      accessKeyId = rootConfig.asString("access-key-id"),
      secretAccessKey = rootConfig.asString("secret-access-key"),
      endpoint = rootConfig.asString("endpoint"),
      region = rootConfig.asString("region"),
      maxConcurrency = rootConfig.asInt("max-concurrency"),
      maxPendingConnectionAcquires = rootConfig.asInt("max-pending-connection-acquires"),
      readTimeout = rootConfig.asFiniteDuration("read-timeout"),
      writeTimeout = rootConfig.asFiniteDuration("write-timeout"),
      connectionTimeout = rootConfig.asFiniteDuration("connection-timeout"),
      connectionAcquisitionTimeout = rootConfig.asFiniteDuration("connection-acquisition-timeout"),
      connectionTimeToLive = rootConfig.asFiniteDuration("connection-time-to-live"),
      maxIdleConnectionTimeout = rootConfig.asFiniteDuration("max-idle-connection-timeout"),
      useConnectionReaper = rootConfig.asBoolean("use-connection-reaper"),
      threadsOfEventLoopGroup = rootConfig.asInt("threads-of-event-loop-group"),
      userHttp2 = rootConfig.asBoolean("user-http2"),
      maxHttp2Streams = rootConfig.asInt("max-http2-streams"),
      batchGetItemLimit = rootConfig.asInt("batch-get-item-limit", 100),
      batchWriteItemLimit = rootConfig.asInt("batch-write-item-limit", 25)
    )
    result
  }

}

case class DynamoDBClientConfig(
    accessKeyId: Option[String],
    secretAccessKey: Option[String],
    endpoint: Option[String],
    region: Option[String],
    maxConcurrency: Option[Int],
    maxPendingConnectionAcquires: Option[Int],
    readTimeout: Option[FiniteDuration],
    writeTimeout: Option[FiniteDuration],
    connectionTimeout: Option[FiniteDuration],
    connectionAcquisitionTimeout: Option[FiniteDuration],
    connectionTimeToLive: Option[FiniteDuration],
    maxIdleConnectionTimeout: Option[FiniteDuration],
    useConnectionReaper: Option[Boolean],
    threadsOfEventLoopGroup: Option[Int],
    userHttp2: Option[Boolean],
    maxHttp2Streams: Option[Int],
    batchGetItemLimit: Int,
    batchWriteItemLimit: Int
) {
  require(batchGetItemLimit >= 1 && batchGetItemLimit <= 100)
  require(batchWriteItemLimit >= 1 && batchWriteItemLimit <= 25)
}
