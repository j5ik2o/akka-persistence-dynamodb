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

import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._

object BackoffConfig extends LoggingSupport {

  val enabledKey      = "enabled"
  val minBackoffKey   = "min-backoff"
  val maxBackoffKey   = "max-backoff"
  val randomFactorKey = "random-factor"
  val maxRestartsKey  = "max-restarts"

  val DefaultEnabled                    = false
  val DefaultMinBackoff: FiniteDuration = 3.seconds
  val DefaultMaxBackoff: FiniteDuration = 30.seconds
  val DefaultRandomFactor: Double       = 0.8
  val DefaultMaxRestarts: Int           = 3

  def fromConfig(config: Config): BackoffConfig = {
    logger.debug("config = {}", config)
    val result = BackoffConfig(
      sourceConfig = config,
      enabled = config.getOrElse(enabledKey, DefaultEnabled),
      minBackoff = config.getOrElse[FiniteDuration](minBackoffKey, DefaultMinBackoff),
      maxBackoff = config.getOrElse[FiniteDuration](maxBackoffKey, DefaultMaxBackoff),
      randomFactor = config.getOrElse[Double](randomFactorKey, DefaultRandomFactor),
      maxRestarts = config.getOrElse[Int](maxRestartsKey, DefaultMaxRestarts)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class BackoffConfig(
    sourceConfig: Config,
    enabled: Boolean,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    maxRestarts: Int
)
