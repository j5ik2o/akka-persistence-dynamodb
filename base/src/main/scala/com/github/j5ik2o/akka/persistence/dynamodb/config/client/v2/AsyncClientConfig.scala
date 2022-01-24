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

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._

object AsyncClientConfig extends LoggingSupport {
  val maxConcurrencyKey               = "max-concurrency"
  val maxPendingConnectionAcquiresKey = "max-pending-connection-acquires"
  val readTimeoutKey                  = "read-timeout"
  val writeTimeoutKey                 = "write-timeout"
  val connectionTimeoutKey            = "connection-timeout"
  val connectionAcquisitionTimeoutKey = "connection-acquisition-timeout"
  val connectionTimeToLiveKey         = "connection-time-to-live"
  val maxIdleConnectionTimeoutKey     = "max-idle-connection-timeout"
  val useConnectionReaperKey          = "use-connection-reaper"
  val threadsOfEventLoopGroupKey      = "threads-of-event-loop-group"
  val useHttp2Key                     = "use-http2"
  val http2MaxStreamsKey              = "http2-max-streams"
  val http2InitialWindowSizeKey       = "http2-initial-window-size"
  val http2HealthCheckPingPeriodKey   = "http2-health-check-ping-period"

  val DefaultMaxConcurrency: Int                          = 50
  val DefaultMaxPendingConnectionAcquires: Int            = 10000
  val DefaultReadTimeout: FiniteDuration                  = 30.seconds
  val DefaultWriteTimeout: FiniteDuration                 = 30.seconds
  val DefaultConnectionTimeout: FiniteDuration            = 2.seconds
  val DefaultConnectionAcquisitionTimeout: FiniteDuration = 10.seconds
  val DefaultConnectionTimeToLive: FiniteDuration         = Duration.Zero
  val DefaultMaxIdleConnectionTimeout: FiniteDuration     = 60.seconds
  val DefaultUseConnectionReaperKey: Boolean              = true
  val DefaultUseHttp2Key: Boolean                         = false
  val DefaultHttp2MaxStreams: Long                        = 4294967295L
  val DefaultHttp2InitialWindowSize: Int                  = 1048576

  private val keyNames =
    Seq(
      maxConcurrencyKey,
      maxPendingConnectionAcquiresKey,
      readTimeoutKey,
      readTimeoutKey,
      connectionTimeoutKey,
      connectionAcquisitionTimeoutKey,
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
    keyNames.map(v => (v, config.exists(v))).toMap
  }

  def fromConfig(config: Config): AsyncClientConfig = {
    logger.debug("config = {}", config)
    val result = AsyncClientConfig(
      sourceConfig = config,
      maxConcurrency = config.getOrElse[Int](maxConcurrencyKey, DefaultMaxConcurrency),
      maxPendingConnectionAcquires =
        config.getOrElse[Int](maxPendingConnectionAcquiresKey, DefaultMaxPendingConnectionAcquires),
      readTimeout = config.getOrElse[FiniteDuration](readTimeoutKey, DefaultReadTimeout),
      writeTimeout = config.getOrElse[FiniteDuration](writeTimeoutKey, DefaultWriteTimeout),
      connectionTimeout = config.getOrElse[FiniteDuration](connectionTimeoutKey, DefaultConnectionTimeout),
      connectionAcquisitionTimeout =
        config.getOrElse[FiniteDuration](connectionAcquisitionTimeoutKey, DefaultConnectionAcquisitionTimeout),
      connectionTimeToLive = config.getOrElse[FiniteDuration](connectionTimeToLiveKey, DefaultConnectionTimeToLive),
      maxIdleConnectionTimeout =
        config.getOrElse[FiniteDuration](maxIdleConnectionTimeoutKey, DefaultMaxIdleConnectionTimeout),
      useConnectionReaper = config.getOrElse[Boolean](useConnectionReaperKey, DefaultUseConnectionReaperKey),
      threadsOfEventLoopGroup = config.getAs[Int](threadsOfEventLoopGroupKey),
      useHttp2 = config.getOrElse[Boolean](useHttp2Key, DefaultUseHttp2Key),
      http2MaxStreams = config.getOrElse[Long](http2MaxStreamsKey, DefaultHttp2MaxStreams),
      http2InitialWindowSize = config.getOrElse[Int](http2InitialWindowSizeKey, DefaultHttp2InitialWindowSize),
      http2HealthCheckPingPeriod = config.getAs[FiniteDuration](http2HealthCheckPingPeriodKey)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class AsyncClientConfig(
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
