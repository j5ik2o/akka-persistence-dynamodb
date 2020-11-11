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

import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._

object SyncClientConfig extends LoggingSupport {

  val dispatcherNameKey               = "dispatcher-name"
  val socketTimeoutKey                = "socket-timeout"
  val connectionTimeoutKey            = "connection-timeout"
  val connectionAcquisitionTimeoutKey = "connection-acquisition-timeout"
  val maxConnectionsKey               = "max-connections"
  val localAddressKey                 = "local-address"
  val expectContinueEnabledKey        = "expect-continue-enabled"
  val connectionTimeToLiveKey         = "connection-time-to-live"
  val maxIdleConnectionTimeoutKey     = "max-idle-connection-timeout"
  val useConnectionReaperKey          = "use-connection-reaper"

  val DefaultSocketTimeout: FiniteDuration                = 50 seconds
  val DefaultConnectionTimeout: FiniteDuration            = 2 seconds
  val DefaultConnectionAcquisitionTimeout: FiniteDuration = 10 seconds
  val DefaultMaxConnections: Int                          = 50
  val DefaultConnectionTimeToLive: FiniteDuration         = Duration.Zero
  val DefaultMaxIdleConnectionTimeout: FiniteDuration     = 60 seconds
  val DefaultUseConnectionReaper: Boolean                 = true

  def fromConfig(config: Config): SyncClientConfig = {
    logger.debug("config = {}", config)
    val result = SyncClientConfig(
      sourceConfig = config,
      dispatcherName = config.getAs[String](dispatcherNameKey),
      socketTimeout = config.getOrElse[FiniteDuration](socketTimeoutKey, DefaultSocketTimeout),
      connectionTimeout = config.getOrElse[FiniteDuration](connectionTimeoutKey, DefaultConnectionTimeout),
      connectionAcquisitionTimeout =
        config.getOrElse[FiniteDuration](connectionAcquisitionTimeoutKey, DefaultConnectionAcquisitionTimeout),
      maxConnections = config.getOrElse[Int](maxConnectionsKey, DefaultMaxConnections),
      localAddress = config.getAs[String](localAddressKey),
      expectContinueEnabled = config.getAs[Boolean](expectContinueEnabledKey),
      connectionTimeToLive = config.getOrElse[FiniteDuration](connectionTimeToLiveKey, DefaultConnectionTimeToLive),
      maxIdleConnectionTimeout =
        config.getOrElse[FiniteDuration](maxIdleConnectionTimeoutKey, DefaultMaxIdleConnectionTimeout),
      useConnectionReaper = config.getOrElse[Boolean](useConnectionReaperKey, DefaultUseConnectionReaper)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class SyncClientConfig(
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
    useConnectionReaper: Boolean
)
