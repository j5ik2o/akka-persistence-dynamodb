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
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
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
      dispatcherName = config.valueOptAs(CommonConfigKeys.dispatcherNameKey),
      connectionTimeout = config.value[FiniteDuration](CommonConfigKeys.connectionTimeoutKey),
      requestTimeout = config.value[FiniteDuration](CommonConfigKeys.requestTimeoutKey),
      healthCheckTimeout = config.value[FiniteDuration](healthCheckTimeoutKey),
      healthCheckInterval = config.value[FiniteDuration](healthCheckIntervalKey),
      idleConnectionTimeout = config.value[FiniteDuration](idleConnectionTimeoutKey),
      minIdleConnectionSize = config.value[Int](minIdleConnectionSizeKey),
      writeRetries = config.value[Int](CommonConfigKeys.writeRetriesKey),
      maxPendingConnectionsPerHost = config.value[Int](maxPendingConnectionsPerHostKey),
      readRetries = config.value[Int](CommonConfigKeys.readRetriesKey),
      threadKeepAlive = config.value[FiniteDuration](threadKeepAliveKey),
      clusterUpdateInterval = config.value[FiniteDuration](clusterUpdateIntervalKey),
      clusterUpdateThreshold = config.value[FiniteDuration](clusterUpdateThresholdKey),
      maxRetryDelay = config.value[FiniteDuration](maxRetryDelayKey),
      unhealthyConsecutiveErrorCount = config.value[Int](unhealthyConsecutiveErrorCountKey),
      awsCredentialsProviderProviderClassName = {
        val className = config.value[String](V1CommonConfigKeys.awsCredentialsProviderProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          V1CommonConfigKeys.AWSCredentialsProviderProviderClassName,
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderClassName = {
        val className = config.valueOptAs[String](V1CommonConfigKeys.awsCredentialsProviderClassNameKey)
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
