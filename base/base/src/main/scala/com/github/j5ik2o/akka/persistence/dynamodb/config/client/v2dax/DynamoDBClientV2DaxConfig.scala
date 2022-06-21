package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2dax

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{
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

  def fromConfig(config: Config, classNameValidation: Boolean): DynamoDBClientV2DaxConfig = {
    logger.debug("config = {}", config)
    val result = new DynamoDBClientV2DaxConfig(
      dispatcherName = config.valueOptAs[String](CommonConfigKeys.dispatcherNameKey),
      idleTimeout = config.value[FiniteDuration](idleTimeoutKey),
      connectionTtl = config.value[FiniteDuration](CommonConfigKeys.connectionTtlKey),
      connectionTimeout = config.value[FiniteDuration](CommonConfigKeys.connectionTimeoutKey),
      requestTimeout = config.value[FiniteDuration](CommonConfigKeys.requestTimeoutKey),
      writeRetries = config.value[Int](CommonConfigKeys.writeRetriesKey),
      readRetries = config.value[Int](CommonConfigKeys.readRetriesKey),
      clusterUpdateInterval = config.value[FiniteDuration](clusterUpdateIntervalKey),
      endpointRefreshTimeout = config.value[FiniteDuration](endpointRefreshTimeoutKey),
      maxPendingConnectionAcquires = config.value[Int](V2CommonConfigKeys.maxPendingConnectionAcquiresKey),
      maxConcurrency = config.value[Int](V2CommonConfigKeys.maxConcurrencyKey),
      skipHostNameVerification = config.value[Boolean](skipHostNameVerificationKey),
      urlOpt = config.valueOptAs[String](urlKey),
      metricPublishersProviderClassName = {
        val className = config.value[String](V2CommonConfigKeys.metricPublisherProviderClassNameKey)
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
        val className = config.value[String](V2CommonConfigKeys.awsCredentialsProviderProviderClassNameKey)
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
