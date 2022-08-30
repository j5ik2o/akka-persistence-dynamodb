package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2dax

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{
  CommonConfigKeys,
  V2CommonConfigDefaultValues,
  V2CommonConfigKeys
}
import net.ceedubs.ficus.Ficus._
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
      dispatcherName = config.getAs[String](CommonConfigKeys.dispatcherNameKey),
      idleTimeout = config.as[FiniteDuration](idleTimeoutKey),
      connectionTtl = config.as[FiniteDuration](CommonConfigKeys.connectionTtlKey),
      connectionTimeout = config.as[FiniteDuration](CommonConfigKeys.connectionTimeoutKey),
      requestTimeout = config.as[FiniteDuration](CommonConfigKeys.requestTimeoutKey),
      writeRetries = config.as[Int](CommonConfigKeys.writeRetriesKey),
      readRetries = config.as[Int](CommonConfigKeys.readRetriesKey),
      clusterUpdateInterval = config.as[FiniteDuration](clusterUpdateIntervalKey),
      endpointRefreshTimeout = config.as[FiniteDuration](endpointRefreshTimeoutKey),
      maxPendingConnectionAcquires = config.as[Int](V2CommonConfigKeys.maxPendingConnectionAcquiresKey),
      maxConcurrency = config.as[Int](V2CommonConfigKeys.maxConcurrencyKey),
      skipHostNameVerification = config.as[Boolean](skipHostNameVerificationKey),
      urlOpt = config.getAs[String](urlKey),
      metricPublishersProviderClassName = {
        val className = config.as[String](V2CommonConfigKeys.metricPublisherProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          V2CommonConfigDefaultValues.MetricPublishersProviderClassName,
          className,
          classNameValidation
        )
      },
      metricPublisherClassNames = {
        val classNames = config.getAs[Seq[String]](V2CommonConfigKeys.metricPublisherClassNameKey).getOrElse(Seq.empty)
        classNames.map { className =>
          ClassCheckUtils
            .requireClassByName(V2CommonConfigDefaultValues.MetricPublisherClassName, className, classNameValidation)
        }
      },
      awsCredentialsProviderProviderClassName = {
        val className = config.as[String](V2CommonConfigKeys.awsCredentialsProviderProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          V2CommonConfigDefaultValues.AwsCredentialsProviderProviderClassName,
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderClassName = {
        val className = config.getAs[String](V2CommonConfigKeys.awsCredentialsProviderClassNameKey)
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
