package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.util.concurrent.TimeUnit

import com.amazon.dax.client.dynamodbv2.ClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

object V1DaxClientConfigUtils {

  def setup(dynamoDBClientConfig: DynamoDBClientConfig): ClientConfig = {
    import dynamoDBClientConfig.v1DaxClientConfig._
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
