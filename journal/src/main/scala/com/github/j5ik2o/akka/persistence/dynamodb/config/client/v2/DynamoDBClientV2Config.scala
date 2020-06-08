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

import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.ExecutionInterceptorsProvider
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.RetryPolicyProvider
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.core.retry.RetryMode

import scala.collection.immutable._
import scala.concurrent.duration.FiniteDuration

object DynamoDBClientV2Config extends LoggingSupport {

  val dispatcherNameKey                        = "dispatcher-name"
  val asyncKey                                 = "async"
  val syncKey                                  = "sync"
  val headersKey                               = "headers"
  val retryModeKey                             = "retry-mode"
  val retryPolicyProviderClassNameKey          = "retry-policy-provider-class-name"
  val executionInterceptorClassNamesKey        = "execution-interceptor-class-names"
  val executionInterceptorProviderClassNameKey = "execution-interceptor-provider-class-name"
  val apiCallTimeoutKey                        = "api-call-timeout"
  val apiCallAttemptTimeoutKey                 = "api-call-attempt-timeout"

  val keyNames: Seq[String] =
    Seq(dispatcherNameKey, asyncKey, syncKey, retryModeKey, apiCallTimeoutKey, apiCallAttemptTimeoutKey)

  def existsKeyNames(config: Config): Map[String, Boolean] = {
    keyNames.map(v => (v, config.exists(v))).toMap
  }

  def fromConfig(config: Config, legacy: Boolean): DynamoDBClientV2Config = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV2Config(
      sourceConfig = config,
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
      retryPolicyProviderClassName = {
        val className = config
          .getAs[String](retryPolicyProviderClassNameKey).orElse(Some(classOf[RetryPolicyProvider.Default].getName))
        ClassCheckUtils.requireClass(classOf[RetryPolicyProvider], className)
      },
      executionInterceptorClassNames = {
        val classNames = config.getOrElse[Seq[String]](executionInterceptorClassNamesKey, Seq.empty)
        classNames.map(s => ClassCheckUtils.requireClass(classOf[ExecutionInterceptor], s))
      },
      executionInterceptorsProviderClassName = {
        val className = config.getOrElse[String](
          executionInterceptorProviderClassNameKey,
          classOf[ExecutionInterceptorsProvider.Default].getName
        )
        ClassCheckUtils.requireClass(classOf[ExecutionInterceptorsProvider], className)
      },
      apiCallTimeout = config.getAs[FiniteDuration](apiCallTimeoutKey),
      apiCallAttemptTimeout = config.getAs[FiniteDuration](apiCallAttemptTimeoutKey)
    )
    logger.debug("result = {}", result)
    result
  }
}

case class DynamoDBClientV2Config(
    sourceConfig: Config,
    dispatcherName: Option[String],
    asyncClientConfig: AsyncClientConfig,
    syncClientConfig: SyncClientConfig,
    headers: Map[String, Seq[String]],
    retryMode: Option[RetryMode],
    retryPolicyProviderClassName: Option[String],
    executionInterceptorClassNames: Seq[String],
    executionInterceptorsProviderClassName: String,
    apiCallTimeout: Option[FiniteDuration],
    apiCallAttemptTimeout: Option[FiniteDuration]
)
