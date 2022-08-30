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

object SyncClientConfig extends LoggingSupport {

  val localAddressKey             = "local-address"
  val expectContinueEnabledKey    = "expect-continue-enabled"
  val connectionTimeToLiveKey     = "connection-time-to-live"
  val maxIdleConnectionTimeoutKey = "max-idle-connection-timeout"
  val useTcpKeepAliveKey          = "use-tcp-keep-alive"
  val useConnectionReaperKey      = "use-connection-reaper"

  def fromConfig(config: Config): SyncClientConfig = {
    logger.debug("config = {}", config)
    val result = SyncClientConfig(
      sourceConfig = config,
      dispatcherName = config.getAs[String](CommonConfigKeys.dispatcherNameKey),
      socketTimeout = config
        .as[FiniteDuration](CommonConfigKeys.socketTimeoutKey),
      connectionTimeout = config.as[FiniteDuration](CommonConfigKeys.connectionTimeoutKey),
      connectionAcquisitionTimeout = config.as[FiniteDuration](V2CommonConfigKeys.connectionAcquisitionTimeoutKey),
      maxConnections = config.as[Int](CommonConfigKeys.maxConnectionsKey),
      localAddress = config.getAs[String](localAddressKey),
      expectContinueEnabled = config.getAs[Boolean](expectContinueEnabledKey),
      connectionTimeToLive = config.as[FiniteDuration](connectionTimeToLiveKey),
      maxIdleConnectionTimeout = config.as[FiniteDuration](maxIdleConnectionTimeoutKey),
      useTcpKeepAlive = config.as[Boolean](useTcpKeepAliveKey),
      useConnectionReaper = config.as[Boolean](useConnectionReaperKey)
    )
    logger.debug("result = {}", result)
    result
  }
}

final case class SyncClientConfig(
    sourceConfig: Config,
    dispatcherName: Option[String],
    socketTimeout: FiniteDuration,
    connectionTimeout: FiniteDuration,
    connectionAcquisitionTimeout: FiniteDuration,
    maxConnections: Int,
    localAddress: Option[String],
    expectContinueEnabled: Option[Boolean],
    connectionTimeToLive: FiniteDuration,
    maxIdleConnectionTimeout: FiniteDuration,
    useTcpKeepAlive: Boolean,
    useConnectionReaper: Boolean
)
