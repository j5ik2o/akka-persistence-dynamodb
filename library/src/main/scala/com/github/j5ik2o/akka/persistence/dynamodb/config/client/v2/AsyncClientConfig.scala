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
      maxConcurrency = config.getOrElse[Int](maxConcurrencyKey, 50),
      maxPendingConnectionAcquires = config.getOrElse[Int](maxPendingConnectionAcquiresKey, 10000),
      readTimeout = config.getOrElse[FiniteDuration](readTimeoutKey, 30 seconds),
      writeTimeout = config.getOrElse[FiniteDuration](writeTimeoutKey, 30 seconds),
      connectionTimeout = config.getOrElse[FiniteDuration](connectionTimeoutKey, 2 seconds),
      connectionAcquisitionTimeout = config.getOrElse[FiniteDuration](connectionAcquisitionTimeoutKey, 10 seconds),
      connectionTimeToLive = config.getOrElse[FiniteDuration](connectionTimeToLiveKey, Duration.Zero),
      maxIdleConnectionTimeout = config.getOrElse[FiniteDuration](maxIdleConnectionTimeoutKey, 60 seconds),
      useConnectionReaper = config.getOrElse[Boolean](useConnectionReaperKey, true),
      threadsOfEventLoopGroup = config.getAs[Int](threadsOfEventLoopGroupKey),
      useHttp2 = config.getOrElse[Boolean](useHttp2Key, false),
      http2MaxStreams = config.getOrElse[Long](http2MaxStreamsKey, 4294967295L),
      http2InitialWindowSize = config.getOrElse[Int](http2InitialWindowSizeKey, 1048576),
      http2HealthCheckPingPeriod = config.getAs[FiniteDuration](http2HealthCheckPingPeriodKey)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class AsyncClientConfig(
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
