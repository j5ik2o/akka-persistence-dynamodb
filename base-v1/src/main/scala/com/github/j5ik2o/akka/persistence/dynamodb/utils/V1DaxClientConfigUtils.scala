package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.amazon.dax.client.dynamodbv2.ClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

private[utils] object V1DaxClientConfigUtils {

  def setup(dynamoDBClientConfig: DynamoDBClientConfig): ClientConfig = {
    import dynamoDBClientConfig.v1DaxClientConfig._
    val result = new ClientConfig()
    if (connectionTimeout != Duration.Zero)
      result
        .setConnectTimeout(
          connectionTimeout.toMillis,
          TimeUnit.MILLISECONDS
        )
    if (requestTimeout != Duration.Zero)
      result
        .setRequestTimeout(
          requestTimeout.toMillis,
          TimeUnit.MILLISECONDS
        )
    if (healthCheckTimeout != Duration.Zero)
      result.setHealthCheckTimeout(
        healthCheckTimeout.toMillis,
        TimeUnit.MILLISECONDS
      )
    if (healthCheckInterval != Duration.Zero)
      result.setHealthCheckInterval(
        healthCheckInterval.toMillis,
        TimeUnit.MILLISECONDS
      )
    if (idleConnectionTimeout != Duration.Zero)
      result.setIdleConnectionTimeout(
        idleConnectionTimeout.toMillis,
        TimeUnit.MILLISECONDS
      )
    result.setMinIdleConnectionSize(
      minIdleConnectionSize
    )

    result.setWriteRetries(
      writeRetries
    )
    result
      .setMaxPendingConnectsPerHost(
        maxPendingConnectionsPerHost
      )
    result.setReadRetries(
      readRetries
    )
    if (threadKeepAlive != Duration.Zero)
      result
        .setThreadKeepAlive(
          threadKeepAlive.toMillis,
          TimeUnit.MILLISECONDS
        )
    if (clusterUpdateInterval != Duration.Zero)
      result
        .setClusterUpdateInterval(
          clusterUpdateInterval.toMillis,
          TimeUnit.MILLISECONDS
        )
    if (clusterUpdateThreshold != Duration.Zero)
      result
        .setClusterUpdateThreshold(
          clusterUpdateThreshold.toMillis,
          TimeUnit.MILLISECONDS
        )
    if (maxRetryDelay != Duration.Zero)
      result
        .setMaxRetryDelay(
          maxRetryDelay.toMillis,
          TimeUnit.MILLISECONDS
        )
    result.setUnhealthyConsecutiveErrorCount(
      unhealthyConsecutiveErrorCount
    )
    result
  }
}
