package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import software.amazon.awssdk.core.retry.RetryMode

import scala.concurrent.duration.{ FiniteDuration, _ }

object DynamoDBClientV2Config extends LoggingSupport {

  def from(config: Config, legacy: Boolean): DynamoDBClientV2Config = {
    val result = DynamoDBClientV2Config(
      dispatcherName = config.asString("dispatcher-name"),
      asyncClientConfig = {
        if (legacy) {
          logger.warn("[[Please migration to the new config format.]]")
          AsyncClientConfig.from(config)
        } else
          AsyncClientConfig.from(config.asConfig("async"))
      },
      syncClientConfig = SyncClientConfig.from(config.asConfig("sync")),
      retryMode = config.asString("retry-mode").map(v => RetryMode.valueOf(v)),
      apiCallTimeout = config.asFiniteDuration("api-call-timeout"),
      apiCallAttemptTimeout = config.asFiniteDuration("api-call-attempt-timeout")
    )
    logger.debug("config = {}", result)
    result
  }
}

case class DynamoDBClientV2Config(
    dispatcherName: Option[String],
    asyncClientConfig: AsyncClientConfig,
    syncClientConfig: SyncClientConfig,
    retryMode: Option[RetryMode],
    apiCallTimeout: Option[FiniteDuration],
    apiCallAttemptTimeout: Option[FiniteDuration]
)

object AsyncClientConfig extends LoggingSupport {

  def from(config: Config): AsyncClientConfig = {
    val result = AsyncClientConfig(
      maxConcurrency = config.asInt("max-concurrency", 50),
      maxPendingConnectionAcquires = config.asInt("max-pending-connection-acquires", 10000),
      readTimeout = config.asFiniteDuration("read-timeout", 30 seconds),
      writeTimeout = config.asFiniteDuration("write-timeout", 30 seconds),
      connectionTimeout = config.asFiniteDuration("connection-timeout", 2 seconds),
      connectionAcquisitionTimeout = config.asFiniteDuration("connection-acquisition-timeout", 10 seconds),
      connectionTimeToLive = config.asFiniteDuration("connection-time-to-live", Duration.Zero),
      maxIdleConnectionTimeout = config.asFiniteDuration("max-idle-connection-timeout", 60 seconds),
      useConnectionReaper = config.asBoolean("use-connection-reaper", true),
      threadsOfEventLoopGroup = config.asInt("threads-of-event-loop-group"),
      useHttp2 = config.asBoolean("use-http2", false),
      http2MaxStreams = config.asLong("http2-max-streams", 4294967295L),
      http2InitialWindowSize = config.asInt("http2-initial-window-size", 1048576),
      http2HealthCheckPingPeriod = config.asFiniteDuration("http2-health-check-ping-period")
    )
    logger.debug("config = {}", result)
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

object SyncClientConfig extends LoggingSupport {

  def from(config: Config): SyncClientConfig = {
    val result = SyncClientConfig(
      socketTimeout = config.asFiniteDuration("socket-timeout", 50 seconds),
      connectionTimeout = config.asFiniteDuration("connection-timeout", 2 seconds),
      connectionAcquisitionTimeout = config.asFiniteDuration("connection-acquisition-timeout", 10 seconds),
      maxConnections = config.asInt("max-connections", 50),
      localAddress = config.asString("local-address"),
      expectContinueEnabled = config.asBoolean("expect-continue-enabled"),
      connectionTimeToLive = config.asFiniteDuration("connection-time-to-live", Duration.Zero),
      maxIdleConnectionTimeout = config.asFiniteDuration("max-idle-connection-timeout", 60 seconds),
      useConnectionReaper = config.asBoolean("use-connection-reaper", true)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class SyncClientConfig(
    socketTimeout: FiniteDuration,
    connectionTimeout: FiniteDuration,
    connectionAcquisitionTimeout: FiniteDuration,
    maxConnections: Int,
    localAddress: Option[String],
    expectContinueEnabled: Option[Boolean],
    connectionTimeToLive: FiniteDuration,
    maxIdleConnectionTimeout: FiniteDuration,
    useConnectionReaper: Boolean
)
