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

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ CommonConfigKeys, V1CommonConfigKeys }
import net.ceedubs.ficus.Ficus._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.Config

import scala.concurrent.duration._

object DynamoDBClientV1DaxConfig extends LoggingSupport {

  val healthCheckTimeoutKey             = "health-check-timeout"
  val healthCheckIntervalKey            = "health-check-interval"
  val idleConnectionTimeoutKey          = "idle-connection-timeout"
  val minIdleConnectionSizeKey          = "min-idle-connection-size"
  val maxPendingConnectionsPerHostKey   = "max-pending-connections-per-host"
  val threadKeepAliveKey                = "thread-keep-alive"
  val clusterUpdateIntervalKey          = "cluster-update-interval"
  val clusterUpdateThresholdKey         = "cluster-update-threshold"
  val maxRetryDelayKey                  = "max-retry-delay"
  val unhealthyConsecutiveErrorCountKey = "unhealthy-consecutive-error-count"

  def fromConfig(config: Config, classNameValidation: Boolean): DynamoDBClientV1DaxConfig = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV1DaxConfig(
      sourceConfig = config,
      dispatcherName = config.getAs[String](CommonConfigKeys.dispatcherNameKey),
      connectionTimeout = config.as[FiniteDuration](CommonConfigKeys.connectionTimeoutKey),
      requestTimeout = config.as[FiniteDuration](CommonConfigKeys.requestTimeoutKey),
      healthCheckTimeout = config.as[FiniteDuration](healthCheckTimeoutKey),
      healthCheckInterval = config.as[FiniteDuration](healthCheckIntervalKey),
      idleConnectionTimeout = config.as[FiniteDuration](idleConnectionTimeoutKey),
      minIdleConnectionSize = config.as[Int](minIdleConnectionSizeKey),
      writeRetries = config.as[Int](CommonConfigKeys.writeRetriesKey),
      maxPendingConnectionsPerHost = config.as[Int](maxPendingConnectionsPerHostKey),
      readRetries = config.as[Int](CommonConfigKeys.readRetriesKey),
      threadKeepAlive = config.as[FiniteDuration](threadKeepAliveKey),
      clusterUpdateInterval = config.as[FiniteDuration](clusterUpdateIntervalKey),
      clusterUpdateThreshold = config.as[FiniteDuration](clusterUpdateThresholdKey),
      maxRetryDelay = config.as[FiniteDuration](maxRetryDelayKey),
      unhealthyConsecutiveErrorCount = config.as[Int](unhealthyConsecutiveErrorCountKey),
      awsCredentialsProviderProviderClassName = {
        val className = config.as[String](V1CommonConfigKeys.awsCredentialsProviderProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          V1CommonConfigKeys.AWSCredentialsProviderProviderClassName,
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderClassName = {
        val className = config.getAs[String](V1CommonConfigKeys.awsCredentialsProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          V1CommonConfigKeys.AWSCredentialsProviderClassName,
          className,
          classNameValidation
        )
      }
    )
    logger.debug("result = {}", result)
    result
  }
}

final case class DynamoDBClientV1DaxConfig(
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
    unhealthyConsecutiveErrorCount: Int,
    awsCredentialsProviderProviderClassName: String,
    awsCredentialsProviderClassName: Option[String]
)
