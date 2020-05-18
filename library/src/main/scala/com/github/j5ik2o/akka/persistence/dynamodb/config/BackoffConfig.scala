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

  def fromConfig(config: Config): BackoffConfig = {
    logger.debug("config = {}", config)
    val result = BackoffConfig(
      enabled = config.getOrElse(enabledKey, false),
      minBackoff = config.getOrElse[FiniteDuration](minBackoffKey, 3 seconds),
      maxBackoff = config.getOrElse[FiniteDuration](maxBackoffKey, 30 seconds),
      randomFactor = config.getOrElse[Double](randomFactorKey, 0.8),
      maxRestarts = config.getOrElse[Int](maxRestartsKey, 20)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class BackoffConfig(
    enabled: Boolean,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    maxRestarts: Int
)
