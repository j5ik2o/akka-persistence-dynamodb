package com.github.j5ik2o.akka.persistence.dynamodb.config.client

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

  val AwsCredentialsProviderProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider"

  val MetricPublishersProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.MetricPublishersProvider"

  val MetricPublisherClassName =
    "software.amazon.awssdk.metrics.MetricPublisher"

  val AwsCredentialsProviderClassName =
    "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider"
}
