package com.github.j5ik2o.akka.persistence.dynamodb.config.client

import scala.concurrent.duration.{ DurationInt, FiniteDuration }

object V2CommonConfigKeys {
  val maxConcurrencyKey               = "max-concurrency"
  val connectionAcquisitionTimeoutKey = "connection-acquisition-timeout"
  val maxPendingConnectionAcquiresKey = "max-pending-connection-acquires"
  val metricPublisherProviderClassNameKey =
    "metric-publishers-provider-class-names"
  val metricPublisherClassNameKey = "metric-publisher-class-names"
  val awsCredentialsProviderProviderClassNameKey =
    "aws-credentials-provider-provider-class-name"
  val awsCredentialsProviderClassNameKey = "aws-credentials-provider-class-name"
}

object V2CommonConfigDefaultValues {
  val DefaultMaxConcurrency: Int               = 50
  val DefaultMaxPendingConnectionAcquires: Int = 10000

  val DefaultConnectionAcquisitionTimeout: FiniteDuration = 10.seconds

  val AwsCredentialsProviderProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider"

  val DefaultAwsCredentialsProviderProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider$Default"

  val MetricPublishersProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.MetricPublishersProvider"

  val DefaultMetricPublishersProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.MetricPublishersProvider$Default"

  val MetricPublisherClassName =
    "software.amazon.awssdk.metrics.MetricPublisher"
  val AwsCredentialsProviderClassName =
    "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider"
}
