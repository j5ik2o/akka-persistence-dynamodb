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
package com.github.j5ik2o.akka.persistence.dynamodb.config

import net.ceedubs.ficus.Ficus._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config

import scala.concurrent.duration._

object BackoffConfig extends LoggingSupport {

  val enabledKey      = "enabled"
  val minBackoffKey   = "min-backoff"
  val maxBackoffKey   = "max-backoff"
  val randomFactorKey = "random-factor"
  val maxRestartsKey  = "max-restarts"

  def fromConfig(config: Config): BackoffConfig = {
    logger.debug("config = {}", config)
    val result = BackoffConfig(
      sourceConfig = config,
      enabled = config.as[Boolean](enabledKey),
      minBackoff = config.as[FiniteDuration](minBackoffKey),
      maxBackoff = config.as[FiniteDuration](maxBackoffKey),
      randomFactor = config.as[Double](randomFactorKey),
      maxRestarts = config.as[Int](maxRestartsKey)
    )
    logger.debug("result = {}", result)
    result
  }
}

final case class BackoffConfig(
    sourceConfig: Config,
    enabled: Boolean,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    maxRestarts: Int
)
