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
      maxConcurrency = rootConfig.asInt("max-concurrency", 50),
      maxPendingConnectionAcquires = rootConfig.asInt("max-pending-connection-acquires", 10000),
      readTimeout = rootConfig.asFiniteDuration("read-timeout", 30 seconds),
      writeTimeout = rootConfig.asFiniteDuration("write-timeout", 30 seconds),
      connectionTimeout = rootConfig.asFiniteDuration("connection-timeout", 2 seconds),
      connectionAcquisitionTimeout = rootConfig.asFiniteDuration("connection-acquisition-timeout", 10 seconds),
      connectionTimeToLive = rootConfig.asFiniteDuration("connection-time-to-live", Duration.Zero),
      maxIdleConnectionTimeout = rootConfig.asFiniteDuration("max-idle-connection-timeout", 60 seconds),
      useConnectionReaper = rootConfig.asBoolean("use-connection-reaper", true),
      threadsOfEventLoopGroup = rootConfig.asInt("threads-of-event-loop-group"),
      useHttp2 = rootConfig.asBoolean("use-http2", false),
      http2MaxStreams = rootConfig.asLong("http2-max-streams", 4294967295L),
      http2InitialWindowSize = rootConfig.asInt("http2-initial-window-size", 1048576),
      http2HealthCheckPingPeriod = rootConfig.asFiniteDuration("http2-health-check-ping-period"),
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
    maxConcurrency: Int,
    maxPendingConnectionAcquires: Int,
    readTimeout: FiniteDuration,
    writeTimeout: FiniteDuration,
    connectionTimeout: FiniteDuration,
    connectionAcquisitionTimeout: FiniteDuration,
    connectionTimeToLive: FiniteDuration,
    maxIdleConnectionTimeout: FiniteDuration,
    useConnectionReaper: Boolean,
    threadsOfEventLoopGroup: Option[Int],
    useHttp2: Boolean,
    http2MaxStreams: Long,
    http2InitialWindowSize: Int,
    http2HealthCheckPingPeriod: Option[FiniteDuration],
    batchGetItemLimit: Int,
    batchWriteItemLimit: Int
) {
  require(batchGetItemLimit >= 1 && batchGetItemLimit <= 100)
  require(batchWriteItemLimit >= 1 && batchWriteItemLimit <= 25)
}
