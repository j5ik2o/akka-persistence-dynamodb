package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2dax

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{
  CommonConfigDefaultValues,
  CommonConfigKeys,
  V2CommonConfigDefaultValues,
  V2CommonConfigKeys
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.Config

import scala.concurrent.duration._

object DynamoDBClientV2DaxConfig extends LoggingSupport {

  val idleTimeoutKey              = "idle-timeout"
  val clusterUpdateIntervalKey    = "cluster-update-interval"
  val endpointRefreshTimeoutKey   = "endpoint-refresh-timeout"
  val skipHostNameVerificationKey = "skip-host-name-verification"
  val urlKey                      = "url"

  val DefaultIdleTimeout: FiniteDuration            = 30000.milliseconds
  val DefaultClusterUpdateInterval: FiniteDuration  = 4000.milliseconds
  val DefaultEndpointRefreshTimeout: FiniteDuration = 6000.milliseconds
  val DefaultMaxConcurrency                         = 1000
  val DefaultSkipHostNameVerification               = false

  def fromConfig(config: Config, classNameValidation: Boolean): DynamoDBClientV2DaxConfig = {
    logger.debug("config = {}", config)
    val result = new DynamoDBClientV2DaxConfig(
      dispatcherName = config.valueOptAs[String](CommonConfigKeys.dispatcherNameKey),
      idleTimeout = config.valueAs[FiniteDuration](idleTimeoutKey, DefaultIdleTimeout),
      connectionTtl = config
        .valueAs[FiniteDuration](CommonConfigKeys.connectionTtlKey, CommonConfigDefaultValues.DefaultConnectionTtl),
      connectionTimeout = config.valueAs[FiniteDuration](
        CommonConfigKeys.connectionTimeoutKey,
        CommonConfigDefaultValues.DefaultConnectionTimeout
      ),
      requestTimeout = config
        .valueAs[FiniteDuration](CommonConfigKeys.requestTimeoutKey, CommonConfigDefaultValues.DefaultRequestTimeout),
      writeRetries =
        config.valueAs[Int](CommonConfigKeys.writeRetriesKey, CommonConfigDefaultValues.DefaultWriteRetries),
      readRetries = config.valueAs[Int](CommonConfigKeys.readRetriesKey, CommonConfigDefaultValues.DefaultReadRetries),
      clusterUpdateInterval = config.valueAs[FiniteDuration](clusterUpdateIntervalKey, DefaultClusterUpdateInterval),
      endpointRefreshTimeout = config.valueAs[FiniteDuration](endpointRefreshTimeoutKey, DefaultEndpointRefreshTimeout),
      maxPendingConnectionAcquires = config.valueAs[Int](
        V2CommonConfigKeys.maxPendingConnectionAcquiresKey,
        V2CommonConfigDefaultValues.DefaultMaxPendingConnectionAcquires
      ),
      maxConcurrency = config.valueAs[Int](V2CommonConfigKeys.maxConcurrencyKey, DefaultMaxConcurrency),
      skipHostNameVerification = config.valueAs[Boolean](skipHostNameVerificationKey, DefaultSkipHostNameVerification),
      urlOpt = config.valueOptAs[String](urlKey),
      metricPublishersProviderClassName = {
        val className = config.valueAs[String](
          V2CommonConfigKeys.metricPublisherProviderClassNameKey,
          V2CommonConfigDefaultValues.DefaultMetricPublishersProviderClassName
        )
        ClassCheckUtils.requireClassByName(
          V2CommonConfigDefaultValues.MetricPublishersProviderClassName,
          className,
          classNameValidation
        )
      },
      metricPublisherClassNames = {
        val classNames = config.valuesAs[String](V2CommonConfigKeys.metricPublisherClassNameKey, Seq.empty)
        classNames.map { className =>
          ClassCheckUtils
            .requireClassByName(V2CommonConfigDefaultValues.MetricPublisherClassName, className, classNameValidation)
        }
      },
      awsCredentialsProviderProviderClassName = {
        val className = config.valueAs[String](
          V2CommonConfigKeys.awsCredentialsProviderProviderClassNameKey,
          V2CommonConfigDefaultValues.DefaultAwsCredentialsProviderProviderClassName
        )
        ClassCheckUtils.requireClassByName(
          V2CommonConfigDefaultValues.AwsCredentialsProviderProviderClassName,
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderClassName = {
        val className = config.valueOptAs[String](V2CommonConfigKeys.awsCredentialsProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          V2CommonConfigDefaultValues.AwsCredentialsProviderClassName,
          className,
          classNameValidation
        )
      }
    )
    logger.debug("result = {}", result)
    result
  }

}

final case class DynamoDBClientV2DaxConfig(
    dispatcherName: Option[String],
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
