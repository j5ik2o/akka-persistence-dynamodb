package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2

import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2.V2RetryPolicyProvider
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._
import software.amazon.awssdk.core.retry.RetryMode

import scala.concurrent.duration.FiniteDuration

object DynamoDBClientV2Config extends LoggingSupport {

  val dispatcherNameKey                 = "dispatcher-name"
  val asyncKey                          = "async"
  val syncKey                           = "sync"
  val headersKey                        = "headers"
  val retryModeKey                      = "retry-mode"
  val retryPolicyProviderClassName      = "retry-policy-provider-class-name"
  val executionInterceptorClassNamesKey = "execution-interceptor-class-names"
  val apiCallTimeoutKey                 = "api-call-timeout"
  val apiCallAttemptTimeoutKey          = "api-call-attempt-timeout"

  val keyNames = Seq(dispatcherNameKey, asyncKey, syncKey, retryModeKey, apiCallTimeoutKey, apiCallAttemptTimeoutKey)

  def existsKeyNames(config: Config): Map[String, Boolean] = {
    keyNames.map(v => (v, config.exists(v))).toMap
  }

  def fromConfig(config: Config, legacy: Boolean): DynamoDBClientV2Config = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV2Config(
      dispatcherName = config.getAs[String](dispatcherNameKey),
      asyncClientConfig = {
        if (legacy) {
          logger.warn(
            "<<<!!!CAUTION: PLEASE MIGRATE TO NEW CONFIG FORMAT!!!>>>\n" +
            "\tThe configuration items of AWS-SDK V2 client remain with the old key names: (j5ik2o.dynamo-db-journal.dynamo-db-client).\n" +
            "\tPlease change current key name to the new key name: (j5ik2o.dynamo-db-journal.dynamo-db-client.v2.async). \n\t" +
            AsyncClientConfig.existsKeyNames(config).filter(_._2).keys.mkString("child-keys = [ ", ", ", " ]")
          )
          AsyncClientConfig.fromConfig(config)
        } else
          AsyncClientConfig.fromConfig(config.getOrElse[Config](asyncKey, ConfigFactory.empty()))
      },
      syncClientConfig = SyncClientConfig.fromConfig(config.getOrElse[Config](syncKey, ConfigFactory.empty())),
      headers = config.getOrElse[Map[String, Seq[String]]](headersKey, Map.empty),
      retryMode = config.getAs[String](retryModeKey).map(s => RetryMode.valueOf(s)),
      retryPolicyProviderClassName = config
        .getAs[String](retryPolicyProviderClassName).orElse(Some(classOf[V2RetryPolicyProvider.Default].getName)),
      executionInterceptorClassNames = config.getOrElse[Seq[String]](executionInterceptorClassNamesKey, Seq.empty),
      apiCallTimeout = config.getAs[FiniteDuration](apiCallTimeoutKey),
      apiCallAttemptTimeout = config.getAs[FiniteDuration](apiCallAttemptTimeoutKey)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class DynamoDBClientV2Config(
    dispatcherName: Option[String],
    asyncClientConfig: AsyncClientConfig,
    syncClientConfig: SyncClientConfig,
    headers: Map[String, Seq[String]],
    retryMode: Option[RetryMode],
    retryPolicyProviderClassName: Option[String],
    executionInterceptorClassNames: Seq[String],
    apiCallTimeout: Option[FiniteDuration],
    apiCallAttemptTimeout: Option[FiniteDuration]
)
