package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2dax

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2.DynamoDBClientV2Config.{
  awsCredentialsProviderClassNameKey,
  awsCredentialsProviderProviderClassNameKey,
  metricPublisherClassNameKey,
  metricPublisherProviderClassNameKey,
  AwsCredentialsProviderClassName,
  AwsCredentialsProviderProviderClassName,
  DefaultAwsCredentialsProviderProviderClassName,
  DefaultMetricPublishersProviderClassName,
  MetricPublisherClassName,
  MetricPublishersProviderClassName
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.Config

import scala.concurrent.duration._

object DynamoDBClientV2DaxConfig extends LoggingSupport {

  val idleTimeoutMillisKey            = "idle-timeout"
  val connectionTtlKey                = "connection-ttl"
  val connectionTimeoutKey            = "connection-timeout"
  val requestTimeoutKey               = "request-timeout"
  val writeRetriesKey                 = "write-retries"
  val readRetriesKey                  = "read-retries"
  val clusterUpdateIntervalKey        = "cluster-update-interval"
  val endpointRefreshTimeoutKey       = "endpoint-refresh-timeout"
  val maxPendingConnectionAcquiresKey = "max-pending-connection-acquires"
  val maxConcurrencyKey               = "max-concurrency"
  val skipHostNameVerificationKey     = "skip-host-name-verification"
  val urlKey                          = "url"

  val DefaultIdleTimeout: FiniteDuration            = 30000.milliseconds
  val DefaultConnectionTtl: FiniteDuration          = Duration.Zero
  val DefaultConnectionTimeout: FiniteDuration      = 1000.milliseconds
  val DefaultRequestTimeout: FiniteDuration         = 60000.milliseconds
  val DefaultWriteRetries: Int                      = 2
  val DefaultReadRetries: Int                       = 2
  val DefaultClusterUpdateInterval: FiniteDuration  = 4000.milliseconds
  val DefaultEndpointRefreshTimeout: FiniteDuration = 6000.milliseconds
  val DefaultMaxPendingConnectionAcquires           = 10000
  val DefaultMaxConcurrency                         = 1000
  val DefaultSkipHostNameVerification               = false

  def fromConfig(config: Config, classNameValidation: Boolean): DynamoDBClientV2DaxConfig = {
    logger.debug("config = {}", config)
    val result = new DynamoDBClientV2DaxConfig(
      idleTimeout = config.valueAs[FiniteDuration](idleTimeoutMillisKey, DefaultIdleTimeout),
      connectionTtl = config.valueAs[FiniteDuration](connectionTtlKey, DefaultConnectionTtl),
      connectionTimeout = config.valueAs[FiniteDuration](connectionTimeoutKey, DefaultConnectionTimeout),
      requestTimeout = config.valueAs[FiniteDuration](requestTimeoutKey, DefaultRequestTimeout),
      writeRetries = config.valueAs[Int](writeRetriesKey, DefaultWriteRetries),
      readRetries = config.valueAs[Int](readRetriesKey, DefaultReadRetries),
      clusterUpdateInterval = config.valueAs[FiniteDuration](clusterUpdateIntervalKey, DefaultClusterUpdateInterval),
      endpointRefreshTimeout = config.valueAs[FiniteDuration](endpointRefreshTimeoutKey, DefaultEndpointRefreshTimeout),
      maxPendingConnectionAcquires =
        config.valueAs[Int](maxPendingConnectionAcquiresKey, DefaultMaxPendingConnectionAcquires),
      maxConcurrency = config.valueAs[Int](maxConcurrencyKey, DefaultMaxConcurrency),
      skipHostNameVerification = config.valueAs[Boolean](skipHostNameVerificationKey, DefaultSkipHostNameVerification),
      urlOpt = config.valueOptAs[String](urlKey),
      metricPublishersProviderClassName = {
        val className = config.valueAs[String](
          metricPublisherProviderClassNameKey,
          DefaultMetricPublishersProviderClassName
        )
        ClassCheckUtils.requireClassByName(
          MetricPublishersProviderClassName,
          className,
          classNameValidation
        )
      },
      metricPublisherClassNames = {
        val classNames = config.valuesAs[String](metricPublisherClassNameKey, Seq.empty)
        classNames.map(s => ClassCheckUtils.requireClassByName(MetricPublisherClassName, s, classNameValidation))
      },
      awsCredentialsProviderProviderClassName = {
        val className = config.valueAs[String](
          awsCredentialsProviderProviderClassNameKey,
          DefaultAwsCredentialsProviderProviderClassName
        )
        ClassCheckUtils.requireClassByName(
          AwsCredentialsProviderProviderClassName,
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderClassName = {
        val className = config.valueOptAs[String](awsCredentialsProviderClassNameKey)
        ClassCheckUtils.requireClassByName(AwsCredentialsProviderClassName, className, classNameValidation)
      }
    )
    logger.debug("result = {}", result)
    result
  }

}

final case class DynamoDBClientV2DaxConfig(
    idleTimeout: FiniteDuration,
    connectionTtl: FiniteDuration,
    connectionTimeout: FiniteDuration,
    requestTimeout: FiniteDuration,
    writeRetries: Int,
    readRetries: Int,
    clusterUpdateInterval: FiniteDuration,
    endpointRefreshTimeout: FiniteDuration,
    maxPendingConnectionAcquires: Int,
    maxConcurrency: Int,
    skipHostNameVerification: Boolean,
    urlOpt: Option[String],
    metricPublishersProviderClassName: String,
    metricPublisherClassNames: scala.collection.Seq[String],
    awsCredentialsProviderProviderClassName: String,
    awsCredentialsProviderClassName: Option[String]
)
