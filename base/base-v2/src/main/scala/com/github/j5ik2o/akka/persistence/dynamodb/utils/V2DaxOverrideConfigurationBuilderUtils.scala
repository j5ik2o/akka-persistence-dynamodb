package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.{
  AwsCredentialsProviderProvider,
  MetricPublishersProvider
}
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.dax.Configuration

import scala.concurrent.duration.Duration

object V2DaxOverrideConfigurationBuilderUtils {

  def setup(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): Configuration.Builder = {
    import pluginConfig.clientConfig.v2DaxClientConfig._
    var builder = Configuration.builder()

    if (idleTimeout != Duration.Zero)
      builder.idleTimeoutMillis(idleTimeout.toMillis.toInt)

    if (connectionTtl != Duration.Zero)
      builder.connectionTtlMillis(connectionTtl.toMillis.toInt)

    if (connectionTimeout != Duration.Zero)
      builder.connectTimeoutMillis(connectionTimeout.toMillis.toInt)

    if (requestTimeout != Duration.Zero)
      builder.requestTimeoutMillis(requestTimeout.toMillis.toInt)

    builder.writeRetries(writeRetries)
    builder.readRetries(readRetries)
    builder.clusterUpdateIntervalMillis(clusterUpdateInterval.toMillis.toInt)
    builder.endpointRefreshTimeoutMillis(endpointRefreshTimeout.toMillis.toInt)
    builder.maxPendingConnectionAcquires(maxPendingConnectionAcquires)
    builder.maxConcurrency(maxConcurrency)
    // builder.eventLoopGroup()
    builder.skipHostNameVerification(skipHostNameVerification)

    (pluginConfig.clientConfig.accessKeyId, pluginConfig.clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder = builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
        val awsCredentialsProviderProvider =
          AwsCredentialsProviderProvider.create(dynamicAccess, pluginConfig)
        awsCredentialsProviderProvider.create.foreach { cp =>
          builder.credentialsProvider(cp)
        }
    }

    pluginConfig.clientConfig.region.foreach { r =>
      builder.region(Region.of(r))
    }
    urlOpt.foreach(url => builder.url(url))

    val metricPublishersProvider = MetricPublishersProvider.create(dynamicAccess, pluginConfig)
    val metricPublishers         = metricPublishersProvider.create
    metricPublishers.foreach { m =>
      builder.addMetricPublisher(m)
    }

    builder
  }
}
