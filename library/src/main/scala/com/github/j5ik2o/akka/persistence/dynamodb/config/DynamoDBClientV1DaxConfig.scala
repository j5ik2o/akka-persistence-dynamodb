package com.github.j5ik2o.akka.persistence.dynamodb.config

import java.util.concurrent.TimeUnit

import com.amazon.dax.client.dynamodbv2.ClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

import scala.concurrent.duration._

object DynamoDBClientV1DaxConfig {

  def from(config: Config): DynamoDBClientV1DaxConfig = {
    DynamoDBClientV1DaxConfig(
      connectionTimeout = config.asFiniteDuration("connection-timeout", 1000 milliseconds),
      requestTimeout = config.asFiniteDuration("request-timeout", 60000 milliseconds),
      healthCheckTimeout = config.asFiniteDuration("health-check-timeout", 1000 milliseconds),
      healthCheckInterval = config.asFiniteDuration("health-check-interval", 5000 milliseconds),
      idleConnectionTimeout = config.asFiniteDuration("idle-connection-timeout", 30000L milliseconds),
      minIdleConnectionSize = config.asInt("min-idle-connection-size", 1),
      writeRetries = config.asInt("write-retries", 2),
      maxPendingConnectionsPerHost = config.asInt("max-pending-connections-per-host", 10),
      readRetries = config.asInt("read-retries", 2),
      threadKeepAlive = config.asFiniteDuration("thread-keep-alive", 10000 milliseconds),
      clusterUpdateInterval = config.asFiniteDuration("cluster-update-interval", 4000 milliseconds),
      clusterUpdateThreshold = config.asFiniteDuration("cluster-update-threshold", 125 milliseconds),
      maxRetryDelay = config.asFiniteDuration("max-retry-delay", 7000 milliseconds),
      unhealthyConsecutiveErrorCount = config.asInt("unhealthy-consecutive-error-count", 5)
    )
  }
}

case class DynamoDBClientV1DaxConfig(
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
) {

  def toAWS: ClientConfig = {
    new ClientConfig()
      .withConnectTimeout(
        connectionTimeout.toMillis,
        TimeUnit.MILLISECONDS
      ).withRequestTimeout(
        requestTimeout.toMillis,
        TimeUnit.MILLISECONDS
      ).withHealthCheckTimeout(
        healthCheckTimeout.toMillis,
        TimeUnit.MILLISECONDS
      ).withHealthCheckInterval(
        healthCheckInterval.toMillis,
        TimeUnit.MILLISECONDS
      ).withIdleConnectionTimeout(
        idleConnectionTimeout.toMillis,
        TimeUnit.MILLISECONDS
      ).withMinIdleConnectionSize(
        minIdleConnectionSize
      ).withWriteRetries(
        writeRetries
      ).withMaxPendingConnectsPerHost(
        maxPendingConnectionsPerHost
      ).withReadRetries(
        readRetries
      ).withThreadKeepAlive(
        threadKeepAlive.toMillis,
        TimeUnit.MILLISECONDS
      ).withClusterUpdateInterval(
        clusterUpdateInterval.toMillis,
        TimeUnit.MILLISECONDS
      ).withClusterUpdateThreshold(
        clusterUpdateThreshold.toMillis,
        TimeUnit.MILLISECONDS
      ).withMaxRetryDelay(
        maxRetryDelay.toMillis,
        TimeUnit.MILLISECONDS
      ).withUnhealthyConsecutiveErrorCount(
        unhealthyConsecutiveErrorCount
      )
  }

}
