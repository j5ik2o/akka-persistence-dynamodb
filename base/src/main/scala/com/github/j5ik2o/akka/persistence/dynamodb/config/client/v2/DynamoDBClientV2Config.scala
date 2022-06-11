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

import com.github.j5ik2o.akka.persistence.dynamodb.config.ConfigSupport._
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.RetryMode
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.immutable._
import scala.concurrent.duration.FiniteDuration

object DynamoDBClientV2Config extends LoggingSupport {

  val dispatcherNameKey                          = "dispatcher-name"
  val asyncKey                                   = "async"
  val syncKey                                    = "sync"
  val headersKey                                 = "headers"
  val retryModeKey                               = "retry-mode"
  val retryPolicyProviderClassNameKey            = "retry-policy-provider-class-name"
  val executionInterceptorClassNamesKey          = "execution-interceptor-class-names"
  val executionInterceptorProviderClassNameKey   = "execution-interceptor-provider-class-name"
  val apiCallTimeoutKey                          = "api-call-timeout"
  val apiCallAttemptTimeoutKey                   = "api-call-attempt-timeout"
  val metricPublisherProviderClassNameKey        = "metric-publishers-provider-class-names"
  val metricPublisherClassNameKey                = "metric-publisher-class-names"
  val awsCredentialsProviderProviderClassNameKey = "aws-credentials-provider-provider-class-name"
  val awsCredentialsProviderClassNameKey         = "aws-credentials-provider-class-name"

  val AwsCredentialsProviderClassName = "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider"
  val MetricPublisherClassName        = "software.amazon.awssdk.metrics.MetricPublisher"

  val keyNames: Seq[String] =
    Seq(dispatcherNameKey, asyncKey, syncKey, retryModeKey, apiCallTimeoutKey, apiCallAttemptTimeoutKey)

  def existsKeyNames(config: Config): Map[String, Boolean] = {
    keyNames.map(v => (v, config.exists(v))).toMap
  }

  val DefaultRetryPolicyProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.RetryPolicyProvider$Default"
  val DefaultExecutionInterceptorsProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.ExecutionInterceptorsProvider$Default"
  val DefaultMetricPublishersProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.MetricPublishersProvider$Default"
  val DefaultAwsCredentialsProviderProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider$Default"

  def fromConfig(config: Config, classNameValidation: Boolean, legacyConfigFormat: Boolean): DynamoDBClientV2Config = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV2Config(
      sourceConfig = config,
      dispatcherName = config.valueOptAs[String](dispatcherNameKey),
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
          AsyncClientConfig.fromConfig(config.configAs(asyncKey, ConfigFactory.empty()))
      },
      syncClientConfig = SyncClientConfig.fromConfig(config.configAs(syncKey, ConfigFactory.empty())),
      headers = config.mapAs[String](headersKey, Map.empty),
      retryPolicyProviderClassName = {
        val className = config
          .valueOptAs[String](retryPolicyProviderClassNameKey).orElse(
            Some(DefaultRetryPolicyProviderClassName)
          )
        ClassCheckUtils
          .requireClassByName(
            "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.RetryPolicyProvider",
            className,
            classNameValidation
          )
      },
      retryMode = config.valueOptAs[String](retryModeKey).map(s => RetryMode.withName(s.toUpperCase)),
      executionInterceptorsProviderClassName = {
        val className = config.valueAs[String](
          executionInterceptorProviderClassNameKey,
          DefaultExecutionInterceptorsProviderClassName
        )
        ClassCheckUtils.requireClassByName(
          "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.ExecutionInterceptorsProvider",
          className,
          classNameValidation
        )
      },
      executionInterceptorClassNames = {
        val classNames = config.valuesAs[String](executionInterceptorClassNamesKey, Seq.empty)
        classNames
          .map(s => ClassCheckUtils.requireClassByName(MetricPublisherClassName, s, classNameValidation)).toIndexedSeq
      },
      apiCallTimeout = config.valueOptAs[FiniteDuration](apiCallTimeoutKey),
      apiCallAttemptTimeout = config.valueOptAs[FiniteDuration](apiCallAttemptTimeoutKey),
      metricPublishersProviderClassName = {
        val className = config.valueAs[String](
          metricPublisherProviderClassNameKey,
          DefaultMetricPublishersProviderClassName
        )
        ClassCheckUtils.requireClassByName(
          "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.MetricPublishersProvider",
          className,
          classNameValidation
        )
      },
      metricPublisherClassNames = {
        val classNames = config.valuesAs[String](metricPublisherClassNameKey, Seq.empty)
        classNames.map(s =>
          ClassCheckUtils.requireClassByName("software.amazon.awssdk.metrics.MetricPublisher", s, classNameValidation)
        )
      },
      awsCredentialsProviderProviderClassName = {
        val className = config.valueAs[String](
          awsCredentialsProviderProviderClassNameKey,
          DefaultAwsCredentialsProviderProviderClassName
        )
        ClassCheckUtils.requireClassByName(
          "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider",
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

final case class DynamoDBClientV2Config(
    sourceConfig: Config,
    dispatcherName: Option[String],
    asyncClientConfig: AsyncClientConfig,
    syncClientConfig: SyncClientConfig,
    headers: scala.collection.Map[String, scala.collection.Seq[String]],
    retryPolicyProviderClassName: Option[String],
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
