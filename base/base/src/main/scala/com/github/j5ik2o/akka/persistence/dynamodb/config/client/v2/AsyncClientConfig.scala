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
package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ CommonConfigKeys, V2CommonConfigKeys }
import net.ceedubs.ficus.Ficus._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config

import scala.concurrent.duration._

object AsyncClientConfig extends LoggingSupport {

  val readTimeoutKey                = "read-timeout"
  val writeTimeoutKey               = "write-timeout"
  val connectionTimeToLiveKey       = "connection-time-to-live"
  val maxIdleConnectionTimeoutKey   = "max-idle-connection-timeout"
  val useConnectionReaperKey        = "use-connection-reaper"
  val threadsOfEventLoopGroupKey    = "threads-of-event-loop-group"
  val useHttp2Key                   = "use-http2"
  val http2MaxStreamsKey            = "http2-max-streams"
  val http2InitialWindowSizeKey     = "http2-initial-window-size"
  val http2HealthCheckPingPeriodKey = "http2-health-check-ping-period"

  private val keyNames =
    Seq(
      V2CommonConfigKeys.maxConcurrencyKey,
      V2CommonConfigKeys.maxPendingConnectionAcquiresKey,
      readTimeoutKey,
      readTimeoutKey,
      CommonConfigKeys.connectionTimeoutKey,
      V2CommonConfigKeys.connectionAcquisitionTimeoutKey,
      connectionTimeToLiveKey,
      maxIdleConnectionTimeoutKey,
      useConnectionReaperKey,
      threadsOfEventLoopGroupKey,
      useHttp2Key,
      http2MaxStreamsKey,
      http2InitialWindowSizeKey,
      http2HealthCheckPingPeriodKey
    )

  def existsKeyNames(config: Config): Map[String, Boolean] = {
    keyNames.map(v => (v, config.hasPath(v))).toMap
  }

  def fromConfig(config: Config): AsyncClientConfig = {
    logger.debug("config = {}", config)
    val result = AsyncClientConfig(
      sourceConfig = config,
      maxConcurrency = config.as[Int](V2CommonConfigKeys.maxConcurrencyKey),
      maxPendingConnectionAcquires = config.as[Int](V2CommonConfigKeys.maxPendingConnectionAcquiresKey),
      readTimeout = config.as[FiniteDuration](readTimeoutKey),
      writeTimeout = config.as[FiniteDuration](writeTimeoutKey),
      connectionTimeout = config.as[FiniteDuration](CommonConfigKeys.connectionTimeoutKey),
      connectionAcquisitionTimeout = config.as[FiniteDuration](V2CommonConfigKeys.connectionAcquisitionTimeoutKey),
      connectionTimeToLive = config.as[FiniteDuration](connectionTimeToLiveKey),
      maxIdleConnectionTimeout = config.as[FiniteDuration](maxIdleConnectionTimeoutKey),
      useConnectionReaper = config.as[Boolean](useConnectionReaperKey),
      threadsOfEventLoopGroup = config.getAs[Int](threadsOfEventLoopGroupKey),
      useHttp2 = config.as[Boolean](useHttp2Key),
      http2MaxStreams = config.as[Long](http2MaxStreamsKey),
      http2InitialWindowSize = config.as[Int](http2InitialWindowSizeKey),
      http2HealthCheckPingPeriod = config.getAs[FiniteDuration](http2HealthCheckPingPeriodKey)
    )
    logger.debug("result = {}", result)
    result
  }
}

final case class AsyncClientConfig(
    sourceConfig: Config,
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
    http2HealthCheckPingPeriod: Option[FiniteDuration]
)
