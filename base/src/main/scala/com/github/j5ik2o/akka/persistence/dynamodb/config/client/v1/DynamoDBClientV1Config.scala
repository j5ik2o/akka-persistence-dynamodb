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
package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1

import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }
import com.github.j5ik2o.akka.persistence.dynamodb.config.ConfigSupport._

import scala.collection.immutable._

object DynamoDBClientV1Config extends LoggingSupport {

  val dispatcherNameKey                            = "dispatcher-name"
  val clientConfigurationKey                       = "client-configuration"
  val requestMetricCollectorProviderClassNameKey   = "request-metric-collector-provider-class-name"
  val requestMetricCollectorClassNameKey           = "request-metric-collector-class-name"
  val monitoringListenerProviderClassNameKey       = "monitoring-listener-provider-class-name"
  val monitoringListenerClassNameKey               = "monitoring-listener-class-name"
  val requestHandlersProviderClassNameKey          = "request-handlers-provider-class-name"
  val requestHandlerClassNamesKey                  = "request-handler-class-names"
  val csmConfigurationProviderProviderClassNameKey = "csm-configuration-provider-provider-class-name"
  val csmConfigurationProviderClassNameKey         = "csm-configuration-provider-class-name"
  val awsCredentialsProviderProviderClassNameKey   = "aws-credentials-provider-provider-class-name"
  val awsCredentialsProviderClassNameKey           = "aws-credentials-provider-class-name"

  val DefaultRequestMetricCollectorProviderClassName: String =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.RequestMetricCollectorProvider$Default" // classOf[RequestMetricCollectorProvider.Default].getName
  val DefaultMonitoringListenerProviderClassName: String =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.MonitoringListenerProvider$Default" // classOf[MonitoringListenerProvider.Default].getName
  val DefaultRequestHandlersProviderClassName: String =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.RequestHandlersProvider$Default"
  val DefaultCsmConfigurationProviderProviderClassName: String =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.CsmConfigurationProviderProvider$Default"
  val DefaultAWSCredentialsProviderProviderClassName: String =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.AWSCredentialsProviderProvider$Default"

  val RequestMetricCollectorClassName   = "com.amazonaws.metrics.RequestMetricCollector"
  val MonitoringListenerClassName       = "com.amazonaws.monitoring.MonitoringListener"
  val RequestHandlerClassName           = "com.amazonaws.handlers.RequestHandler2"
  val CsmConfigurationProviderClassName = "com.amazonaws.monitoring.CsmConfigurationProvider"
  val AWSCredentialsProviderClassName   = "com.amazonaws.auth.AWSCredentialsProvider"

  def fromConfig(config: Config, classNameValidation: Boolean): DynamoDBClientV1Config = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV1Config(
      sourceConfig = config,
      dispatcherName = config.valueOptAs[String](dispatcherNameKey),
      clientConfiguration = ClientConfiguration
        .fromConfig(config.configAs(clientConfigurationKey, ConfigFactory.empty()), classNameValidation),
      requestMetricCollectorProviderClassName = {
        val className =
          config.valueAs(requestMetricCollectorProviderClassNameKey, DefaultRequestMetricCollectorProviderClassName)
        ClassCheckUtils.requireClassByName(
          "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.RequestMetricCollectorProvider",
          className,
          classNameValidation
        )
      },
      requestMetricCollectorClassName = {
        val className = config.valueOptAs[String](requestMetricCollectorClassNameKey)
        ClassCheckUtils.requireClassByName(RequestMetricCollectorClassName, className, classNameValidation)
      },
      monitoringListenerProviderClassName = {
        val className = config
          .valueAs(monitoringListenerProviderClassNameKey, DefaultMonitoringListenerProviderClassName)
        ClassCheckUtils.requireClassByName(
          "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.MonitoringListenerProvider",
          className,
          classNameValidation
        )
      },
      monitoringListenerClassName = {
        val className = config.valueOptAs[String](monitoringListenerClassNameKey)
        ClassCheckUtils.requireClassByName(MonitoringListenerClassName, className, classNameValidation)
      },
      requestHandlersProviderClassName = {
        val className = config
          .valueAs[String](requestHandlersProviderClassNameKey, DefaultRequestHandlersProviderClassName)
        ClassCheckUtils.requireClassByName(
          "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.RequestHandlersProvider",
          className,
          classNameValidation
        )
      },
      requestHandlerClassNames = {
        val classNames = config.valuesAs[String](requestHandlerClassNamesKey, Vector.empty)
        classNames.map { className =>
          ClassCheckUtils.requireClassByName(RequestHandlerClassName, className, classNameValidation)
        }.toIndexedSeq
      },
      csmConfigurationProviderProviderClassName = {
        val className = config
          .valueAs[String](
            csmConfigurationProviderProviderClassNameKey,
            DefaultCsmConfigurationProviderProviderClassName
          )
        ClassCheckUtils.requireClassByName(
          "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.CsmConfigurationProviderProvider",
          className,
          classNameValidation
        )
      },
      csmConfigurationProviderClassName = {
        val className = config.valueOptAs[String](csmConfigurationProviderClassNameKey)
        ClassCheckUtils.requireClassByName(CsmConfigurationProviderClassName, className, classNameValidation)
      },
      awsCredentialsProviderProviderClassName = {
        val className = config
          .valueAs[String](
            awsCredentialsProviderProviderClassNameKey,
            DefaultAWSCredentialsProviderProviderClassName
          )
        ClassCheckUtils.requireClassByName(
          "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.AWSCredentialsProviderProvider",
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderClassName = {
        val className = config.valueOptAs[String](awsCredentialsProviderClassNameKey)
        ClassCheckUtils.requireClassByName("com.amazonaws.auth.AWSCredentialsProvider", className, classNameValidation)
      }
    )
    logger.debug("result = {}", result)
    result
  }
}

final case class DynamoDBClientV1Config(
    sourceConfig: Config,
    dispatcherName: Option[String],
    clientConfiguration: ClientConfiguration,
    requestMetricCollectorProviderClassName: String,
    requestMetricCollectorClassName: Option[String],
    monitoringListenerProviderClassName: String,
    monitoringListenerClassName: Option[String],
    requestHandlersProviderClassName: String,
    requestHandlerClassNames: Seq[String],
    csmConfigurationProviderProviderClassName: String,
    csmConfigurationProviderClassName: Option[String],
    awsCredentialsProviderProviderClassName: String,
    awsCredentialsProviderClassName: Option[String]
)
