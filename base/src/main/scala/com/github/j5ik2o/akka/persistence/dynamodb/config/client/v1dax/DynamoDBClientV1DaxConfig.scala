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

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1.DynamoDBClientV1Config.{
  awsCredentialsProviderClassNameKey,
  awsCredentialsProviderProviderClassNameKey,
  AWSCredentialsProviderClassName,
  AWSCredentialsProviderProviderClassName,
  DefaultAWSCredentialsProviderProviderClassName
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.Config

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

  def fromConfig(config: Config, classNameValidation: Boolean): DynamoDBClientV1DaxConfig = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV1DaxConfig(
      sourceConfig = config,
      dispatcherName = config.valueOptAs(dispatcherNameKey),
      connectionTimeout = config.valueAs[FiniteDuration](connectionTimeoutKey, DefaultConnectionTimeout),
      requestTimeout = config.valueAs[FiniteDuration](requestTimeoutKey, DefaultRequestTimeout),
      healthCheckTimeout = config.valueAs[FiniteDuration](healthCheckTimeoutKey, DefaultHealthCheckTimeout),
      healthCheckInterval = config.valueAs[FiniteDuration](healthCheckIntervalKey, DefaultHealthCheckInterval),
      idleConnectionTimeout = config.valueAs[FiniteDuration](idleConnectionTimeoutKey, DefaultIdleConnectionTimeout),
      minIdleConnectionSize = config.valueAs[Int](minIdleConnectionSizeKey, DefaultMinIdleConnectionSize),
      writeRetries = config.valueAs[Int](writeRetriesKey, DefaultWriteRetriesKey),
      maxPendingConnectionsPerHost =
        config.valueAs[Int](maxPendingConnectionsPerHostKey, DefaultMaxPendingConnectionsPerHost),
      readRetries = config.valueAs[Int](readRetriesKey, DefaultReadRetries),
      threadKeepAlive = config.valueAs[FiniteDuration](threadKeepAliveKey, DefaultThreadKeepAlive),
      clusterUpdateInterval = config.valueAs[FiniteDuration](clusterUpdateIntervalKey, DefaultClusterUpdateInterval),
      clusterUpdateThreshold = config.valueAs[FiniteDuration](clusterUpdateThresholdKey, DefaultClusterUpdateThreshold),
      maxRetryDelay = config.valueAs[FiniteDuration](maxRetryDelayKey, DefaultMaxRetryDelay),
      unhealthyConsecutiveErrorCount =
        config.valueAs[Int](unhealthyConsecutiveErrorCountKey, DefaultUnhealthyConsecutiveErrorCount),
      awsCredentialsProviderProviderClassName = {
        val className = config
          .valueAs[String](
            awsCredentialsProviderProviderClassNameKey,
            DefaultAWSCredentialsProviderProviderClassName
          )
        ClassCheckUtils.requireClassByName(
          AWSCredentialsProviderProviderClassName,
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderClassName = {
        val className = config.valueOptAs[String](awsCredentialsProviderClassNameKey)
        ClassCheckUtils.requireClassByName(AWSCredentialsProviderClassName, className, classNameValidation)
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
