package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import software.amazon.awssdk.core.retry.RetryMode

import scala.concurrent.duration.FiniteDuration

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

object AsyncClientConfig extends LoggingSupport {

  def from(config: Config): AsyncClientConfig = {
    AsyncClientConfig(
      maxConcurrency = config.asInt("max-concurrency"),
      maxPendingConnectionAcquires = config.asInt("max-pending-connection-acquires"),
      readTimeout = config.asFiniteDuration("read-timeout"),
      writeTimeout = config.asFiniteDuration("write-timeout"),
      connectionTimeout = config.asFiniteDuration("connection-timeout"),
      connectionAcquisitionTimeout = config.asFiniteDuration("connection-acquisition-timeout"),
      connectionTimeToLive = config.asFiniteDuration("connection-time-to-live"),
      maxIdleConnectionTimeout = config.asFiniteDuration("max-idle-connection-timeout"),
      useConnectionReaper = config.asBoolean("use-connection-reaper"),
      threadsOfEventLoopGroup = config.asInt("threads-of-event-loop-group"),
      useHttp2 = config.asBoolean("use-http2"),
      http2MaxStreams = config.asLong("http2-max-streams"),
      http2InitialWindowSize = config.asInt("http2-initial-window-size"),
      http2HealthCheckPingPeriod = config.asFiniteDuration("http2-health-check-ping-period")
    )
  }
}

case class AsyncClientConfig(
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
    useHttp2: Option[Boolean],
    http2MaxStreams: Option[Long],
    http2InitialWindowSize: Option[Int],
    http2HealthCheckPingPeriod: Option[FiniteDuration]
)

object SyncClientConfig {

  def from(config: Config): SyncClientConfig = {
    SyncClientConfig(
      maxConnections = config.asInt("max-connections", 50),
      localAddress = config.asString("local-address"),
      expectContinueEnabled = config.asBoolean("expect-continue-enabled"),
      connectionTimeToLive = config.asFiniteDuration("connection-time-to-live"),
      maxIdleConnectionTimeout = config.asFiniteDuration("max-idle-connection-timeout"),
      useConnectionReaper = config.asBoolean("use-connection-reaper")
    )
  }
}

case class SyncClientConfig(
    maxConnections: Int,
    localAddress: Option[String],
    expectContinueEnabled: Option[Boolean],
    connectionTimeToLive: Option[FiniteDuration],
    maxIdleConnectionTimeout: Option[FiniteDuration],
    useConnectionReaper: Option[Boolean]
)

case class DynamoDBClientV2Config(
    dispatcherName: Option[String],
    retryMode: Option[RetryMode],
    asyncClientConfig: AsyncClientConfig,
    syncClientConfig: SyncClientConfig,
    apiCallTimeout: Option[FiniteDuration],
    apiCallAttemptTimeout: Option[FiniteDuration]
)
