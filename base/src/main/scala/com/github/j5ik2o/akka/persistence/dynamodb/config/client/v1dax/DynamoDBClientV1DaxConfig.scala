/*
 * Copyright 2020 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1dax

import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._

object DynamoDBClientV1DaxConfig extends LoggingSupport {

  val dispatcherNameKey                 = "dispatcher-name"
  val connectionTimeoutKey              = "connection-timeout"
  val requestTimeoutKey                 = "request-timeout"
  val healthCheckTimeoutKey             = "health-check-timeout"
  val healthCheckIntervalKey            = "health-check-interval"
  val idleConnectionTimeoutKey          = "idle-connection-timeout"
  val minIdleConnectionSizeKey          = "min-idle-connection-size"
  val writeRetriesKey                   = "write-retries"
  val maxPendingConnectionsPerHostKey   = "max-pending-connections-per-host"
  val readRetriesKey                    = "read-retries"
  val threadKeepAliveKey                = "thread-keep-alive"
  val clusterUpdateIntervalKey          = "cluster-update-interval"
  val clusterUpdateThresholdKey         = "cluster-update-threshold"
  val maxRetryDelayKey                  = "max-retry-delay"
  val unhealthyConsecutiveErrorCountKey = "unhealthy-consecutive-error-count"

  val DefaultConnectionTimeout: FiniteDuration      = 1000.milliseconds
  val DefaultRequestTimeout: FiniteDuration         = 60000.milliseconds
  val DefaultHealthCheckTimeout: FiniteDuration     = 1000.milliseconds
  val DefaultHealthCheckInterval: FiniteDuration    = 5000.milliseconds
  val DefaultIdleConnectionTimeout: FiniteDuration  = 30000.milliseconds
  val DefaultMinIdleConnectionSize: Int             = 1
  val DefaultWriteRetriesKey: Int                   = 2
  val DefaultMaxPendingConnectionsPerHost: Int      = 10
  val DefaultReadRetries: Int                       = 2
  val DefaultThreadKeepAlive: FiniteDuration        = 10000.milliseconds
  val DefaultClusterUpdateInterval: FiniteDuration  = 4000.milliseconds
  val DefaultClusterUpdateThreshold: FiniteDuration = 125.milliseconds
  val DefaultMaxRetryDelay: FiniteDuration          = 7000.milliseconds
  val DefaultUnhealthyConsecutiveErrorCount         = 5

  def fromConfig(config: Config): DynamoDBClientV1DaxConfig = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV1DaxConfig(
      sourceConfig = config,
      dispatcherName = config.getAs[String](dispatcherNameKey),
      connectionTimeout = config.getOrElse[FiniteDuration](connectionTimeoutKey, DefaultConnectionTimeout),
      requestTimeout = config.getOrElse[FiniteDuration](requestTimeoutKey, DefaultRequestTimeout),
      healthCheckTimeout = config.getOrElse[FiniteDuration](healthCheckTimeoutKey, DefaultHealthCheckTimeout),
      healthCheckInterval = config.getOrElse[FiniteDuration](healthCheckIntervalKey, DefaultHealthCheckInterval),
      idleConnectionTimeout = config.getOrElse[FiniteDuration](idleConnectionTimeoutKey, DefaultIdleConnectionTimeout),
      minIdleConnectionSize = config.getOrElse[Int](minIdleConnectionSizeKey, DefaultMinIdleConnectionSize),
      writeRetries = config.getOrElse[Int](writeRetriesKey, DefaultWriteRetriesKey),
      maxPendingConnectionsPerHost =
        config.getOrElse[Int](maxPendingConnectionsPerHostKey, DefaultMaxPendingConnectionsPerHost),
      readRetries = config.getOrElse[Int](readRetriesKey, DefaultReadRetries),
      threadKeepAlive = config.getOrElse[FiniteDuration](threadKeepAliveKey, DefaultThreadKeepAlive),
      clusterUpdateInterval = config.getOrElse[FiniteDuration](clusterUpdateIntervalKey, DefaultClusterUpdateInterval),
      clusterUpdateThreshold =
        config.getOrElse[FiniteDuration](clusterUpdateThresholdKey, DefaultClusterUpdateThreshold),
      maxRetryDelay = config.getOrElse[FiniteDuration](maxRetryDelayKey, DefaultMaxRetryDelay),
      unhealthyConsecutiveErrorCount =
        config.getOrElse[Int](unhealthyConsecutiveErrorCountKey, DefaultUnhealthyConsecutiveErrorCount)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class DynamoDBClientV1DaxConfig(
    sourceConfig: Config,
    dispatcherName: Option[String],
    connectionTimeout: FiniteDuration,
    requestTimeout: FiniteDuration,
    healthCheckTimeout: FiniteDuration,
    healthCheckInterval: FiniteDuration,
    idleConnectionTimeout: FiniteDuration,
    minIdleConnectionSize: Int,
    writeRetries: Int,
    maxPendingConnectionsPerHost: Int,
    readRetries: Int,
    threadKeepAlive: FiniteDuration,
    clusterUpdateInterval: FiniteDuration,
    clusterUpdateThreshold: FiniteDuration,
    maxRetryDelay: FiniteDuration,
    unhealthyConsecutiveErrorCount: Int
) {}
