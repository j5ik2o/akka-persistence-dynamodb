package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1dax

import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._

object DynamoDBClientV1DaxConfig extends LoggingSupport {

  def fromConfig(config: Config): DynamoDBClientV1DaxConfig = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV1DaxConfig(
      dispatcherName = config.getAs[String]("dispatcher-name"),
      connectionTimeout = config.getOrElse[FiniteDuration]("connection-timeout", 1000 milliseconds),
      requestTimeout = config.getOrElse[FiniteDuration]("request-timeout", 60000 milliseconds),
      healthCheckTimeout = config.getOrElse[FiniteDuration]("health-check-timeout", 1000 milliseconds),
      healthCheckInterval = config.getOrElse[FiniteDuration]("health-check-interval", 5000 milliseconds),
      idleConnectionTimeout = config.getOrElse[FiniteDuration]("idle-connection-timeout", 30000L milliseconds),
      minIdleConnectionSize = config.getOrElse[Int]("min-idle-connection-size", 1),
      writeRetries = config.getOrElse[Int]("write-retries", 2),
      maxPendingConnectionsPerHost = config.getOrElse[Int]("max-pending-connections-per-host", 10),
      readRetries = config.getOrElse[Int]("read-retries", 2),
      threadKeepAlive = config.getOrElse[FiniteDuration]("thread-keep-alive", 10000 milliseconds),
      clusterUpdateInterval = config.getOrElse[FiniteDuration]("cluster-update-interval", 4000 milliseconds),
      clusterUpdateThreshold = config.getOrElse[FiniteDuration]("cluster-update-threshold", 125 milliseconds),
      maxRetryDelay = config.getOrElse[FiniteDuration]("max-retry-delay", 7000 milliseconds),
      unhealthyConsecutiveErrorCount = config.getOrElse[Int]("unhealthy-consecutive-error-count", 5)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class DynamoDBClientV1DaxConfig(
    dispatcherName: Option[String],
    connectionTimeout: FiniteDuration,
    requestTimeout: FiniteDuration,
    healthCheckTimeout: FiniteDuration,
    healthCheckInterval: FiniteDuration,
    idleConnectionTimeout: FiniteDuration,
    minIdleConnectionSize: Int,
    writeRetries: Int,
    maxPendingConnectionsPerHost: Int,
    readRetries: Int,
    threadKeepAlive: FiniteDuration,
    clusterUpdateInterval: FiniteDuration,
    clusterUpdateThreshold: FiniteDuration,
    maxRetryDelay: FiniteDuration,
    unhealthyConsecutiveErrorCount: Int
) {}
