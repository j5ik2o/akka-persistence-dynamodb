package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2

import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._

object SyncClientConfig extends LoggingSupport {

  val socketTimeoutKey                = "socket-timeout"
  val connectionTimeoutKey            = "connection-timeout"
  val connectionAcquisitionTimeoutKey = "connection-acquisition-timeout"
  val maxConnectionsKey               = "max-connections"
  val localAddressKey                 = "local-address"
  val expectContinueEnabledKey        = "expect-continue-enabled"
  val connectionTimeToLiveKey         = "connection-time-to-live"
  val maxIdleConnectionTimeoutKey     = "max-idle-connection-timeout"
  val useConnectionReaperKey          = "use-connection-reaper"

  def fromConfig(config: Config): SyncClientConfig = {
    logger.debug("config = {}", config)
    val result = SyncClientConfig(
      socketTimeout = config.getOrElse[FiniteDuration](socketTimeoutKey, 50 seconds),
      connectionTimeout = config.getOrElse[FiniteDuration](connectionTimeoutKey, 2 seconds),
      connectionAcquisitionTimeout = config.getOrElse[FiniteDuration](connectionAcquisitionTimeoutKey, 10 seconds),
      maxConnections = config.getOrElse[Int](maxConnectionsKey, 50),
      localAddress = config.getAs[String](localAddressKey),
      expectContinueEnabled = config.getAs[Boolean](expectContinueEnabledKey),
      connectionTimeToLive = config.getOrElse[FiniteDuration](connectionTimeToLiveKey, Duration.Zero),
      maxIdleConnectionTimeout = config.getOrElse[FiniteDuration](maxIdleConnectionTimeoutKey, 60 seconds),
      useConnectionReaper = config.getOrElse[Boolean](useConnectionReaperKey, true)
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
