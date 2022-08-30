/*
 * Copyright 2020 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{
  CommonConfigKeys,
  RetryMode,
  V2CommonConfigDefaultValues,
  V2CommonConfigKeys
}
import net.ceedubs.ficus.Ficus._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.immutable._
import scala.concurrent.duration.FiniteDuration

object DynamoDBClientV2Config extends LoggingSupport {

  val asyncKey                                 = "async"
  val syncKey                                  = "sync"
  val retryPolicyProviderClassNameKey          = "retry-policy-provider-class-name"
  val executionInterceptorClassNamesKey        = "execution-interceptor-class-names"
  val executionInterceptorProviderClassNameKey = "execution-interceptor-provider-class-name"
  val apiCallTimeoutKey                        = "api-call-timeout"
  val apiCallAttemptTimeoutKey                 = "api-call-attempt-timeout"

  val keyNames: Seq[String] =
    Seq(
      CommonConfigKeys.dispatcherNameKey,
      asyncKey,
      syncKey,
      CommonConfigKeys.retryModeKey,
      apiCallTimeoutKey,
      apiCallAttemptTimeoutKey
    )

  def existsKeyNames(config: Config): Map[String, Boolean] = {
    keyNames.map(v => (v, config.hasPath(v))).toMap
  }

  val RetryPolicyProviderClassName = "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.RetryPolicyProvider"
  val ExecutionInterceptorsProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.ExecutionInterceptorsProvider"

  def fromConfig(config: Config, classNameValidation: Boolean, legacyConfigFormat: Boolean): DynamoDBClientV2Config = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV2Config(
      sourceConfig = config,
      dispatcherName = config.getAs[String](CommonConfigKeys.dispatcherNameKey),
      asyncClientConfig = {
        if (legacyConfigFormat) {
          logger.warn(
            "<<<!!!CAUTION: PLEASE MIGRATE TO NEW CONFIG FORMAT!!!>>>\n" +
            "\tThe configuration items of AWS-SDK V2 client remain with the old key names: (j5ik2o.dynamo-db-journal.dynamo-db-client).\n" +
            "\tPlease change current key name to the new key name: (j5ik2o.dynamo-db-journal.dynamo-db-client.v2.async). \n\t" +
            AsyncClientConfig.existsKeyNames(config).filter(_._2).keys.mkString("child-keys = [ ", ", ", " ]")
          )
          AsyncClientConfig.fromConfig(config)
        } else
          AsyncClientConfig.fromConfig(config.getAs[Config](asyncKey).getOrElse(ConfigFactory.empty()))
      },
      syncClientConfig = SyncClientConfig.fromConfig(config.getAs[Config](syncKey).getOrElse(ConfigFactory.empty())),
      headers = config.getAs[Map[String, Seq[String]]](CommonConfigKeys.headersKey).getOrElse(Map.empty),
      retryPolicyProviderClassName = {
        val className = config.as[String](retryPolicyProviderClassNameKey)
        ClassCheckUtils
          .requireClassByName(
            RetryPolicyProviderClassName,
            className,
            classNameValidation
          )
      },
      retryMode = config.getAs[String](CommonConfigKeys.retryModeKey).map(s => RetryMode.withName(s.toUpperCase)),
      executionInterceptorsProviderClassName = {
        val className = config.as[String](executionInterceptorProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          ExecutionInterceptorsProviderClassName,
          className,
          classNameValidation
        )
      },
      executionInterceptorClassNames = {
        val classNames = config.getAs[Seq[String]](executionInterceptorClassNamesKey).getOrElse(Seq.empty)
        classNames
          .map(s =>
            ClassCheckUtils
              .requireClassByName(V2CommonConfigDefaultValues.MetricPublisherClassName, s, classNameValidation)
          ).toIndexedSeq
      },
      apiCallTimeout = config.getAs[FiniteDuration](apiCallTimeoutKey),
      apiCallAttemptTimeout = config.getAs[FiniteDuration](apiCallAttemptTimeoutKey),
      metricPublishersProviderClassName = {
        val className = config.as[String](
          V2CommonConfigKeys.metricPublisherProviderClassNameKey
        )
        ClassCheckUtils.requireClassByName(
          V2CommonConfigDefaultValues.MetricPublishersProviderClassName,
          className,
          classNameValidation
        )
      },
      metricPublisherClassNames = {
        val classNames = config.getAs[Seq[String]](V2CommonConfigKeys.metricPublisherClassNameKey).getOrElse(Seq.empty)
        classNames.map(s =>
          ClassCheckUtils
            .requireClassByName(V2CommonConfigDefaultValues.MetricPublisherClassName, s, classNameValidation)
        )
      },
      awsCredentialsProviderProviderClassName = {
        val className = config.as[String](
          V2CommonConfigKeys.awsCredentialsProviderProviderClassNameKey
        )
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

final case class DynamoDBClientV2Config(
    sourceConfig: Config,
    dispatcherName: Option[String],
    asyncClientConfig: AsyncClientConfig,
    syncClientConfig: SyncClientConfig,
    headers: scala.collection.Map[String, scala.collection.Seq[String]],
    retryPolicyProviderClassName: String,
    retryMode: Option[RetryMode.Value],
    executionInterceptorsProviderClassName: String,
    executionInterceptorClassNames: Seq[String],
    apiCallTimeout: Option[FiniteDuration],
    apiCallAttemptTimeout: Option[FiniteDuration],
    metricPublishersProviderClassName: String,
    metricPublisherClassNames: scala.collection.Seq[String],
    awsCredentialsProviderProviderClassName: String,
    awsCredentialsProviderClassName: Option[String]
)
