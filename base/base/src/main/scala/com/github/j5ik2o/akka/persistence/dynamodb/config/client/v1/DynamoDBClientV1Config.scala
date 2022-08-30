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

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ CommonConfigKeys, V1CommonConfigKeys }
import net.ceedubs.ficus.Ficus._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.immutable._

object DynamoDBClientV1Config extends LoggingSupport {

  val clientConfigurationKey = "client-configuration"
  val requestMetricCollectorProviderClassNameKey =
    "request-metric-collector-provider-class-name"
  val requestMetricCollectorClassNameKey = "request-metric-collector-class-name"
  val monitoringListenerProviderClassNameKey =
    "monitoring-listener-provider-class-name"
  val monitoringListenerClassNameKey = "monitoring-listener-class-name"
  val requestHandlersProviderClassNameKey =
    "request-handlers-provider-class-name"
  val requestHandlerClassNamesKey = "request-handler-class-names"
  val csmConfigurationProviderProviderClassNameKey =
    "csm-configuration-provider-provider-class-name"
  val csmConfigurationProviderClassNameKey =
    "csm-configuration-provider-class-name"

  val RequestMetricCollectorClassName =
    "com.amazonaws.metrics.RequestMetricCollector"
  val MonitoringListenerClassName =
    "com.amazonaws.monitoring.MonitoringListener"
  val RequestHandlerClassName = "com.amazonaws.handlers.RequestHandler2"
  val CsmConfigurationProviderClassName =
    "com.amazonaws.monitoring.CsmConfigurationProvider"

  val RequestMetricCollectorProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.RequestMetricCollectorProvider"

  val MonitoringListenerProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.MonitoringListenerProvider"

  val RequestHandlersProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.RequestHandlersProvider"

  val CsmConfigurationProviderProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.CsmConfigurationProviderProvider"

  def fromConfig(
      config: Config,
      classNameValidation: Boolean
  ): DynamoDBClientV1Config = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV1Config(
      sourceConfig = config,
      dispatcherName = config.getAs[String](CommonConfigKeys.dispatcherNameKey),
      clientConfiguration = ClientConfiguration
        .fromConfig(
          config.getAs[Config](clientConfigurationKey).getOrElse(ConfigFactory.empty()),
          classNameValidation
        ),
      requestMetricCollectorProviderClassName = {
        val className =
          config.as[String](requestMetricCollectorProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          RequestMetricCollectorProviderClassName,
          className,
          classNameValidation
        )
      },
      requestMetricCollectorClassName = {
        val className =
          config.getAs[String](requestMetricCollectorClassNameKey)
        ClassCheckUtils.requireClassByName(
          RequestMetricCollectorClassName,
          className,
          classNameValidation
        )
      },
      monitoringListenerProviderClassName = {
        val className = config.as[String](monitoringListenerProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          MonitoringListenerProviderClassName,
          className,
          classNameValidation
        )
      },
      monitoringListenerClassName = {
        val className =
          config.getAs[String](monitoringListenerClassNameKey)
        ClassCheckUtils.requireClassByName(
          MonitoringListenerClassName,
          className,
          classNameValidation
        )
      },
      requestHandlersProviderClassName = {
        val className = config.as[String](requestHandlersProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          RequestHandlersProviderClassName,
          className,
          classNameValidation
        )
      },
      requestHandlerClassNames = {
        val classNames =
          config.getAs[Vector[String]](requestHandlerClassNamesKey).getOrElse(Vector.empty)
        classNames.map { className =>
          ClassCheckUtils.requireClassByName(
            RequestHandlerClassName,
            className,
            classNameValidation
          )
        }
      },
      csmConfigurationProviderProviderClassName = {
        val className = config.as[String](csmConfigurationProviderProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          CsmConfigurationProviderProviderClassName,
          className,
          classNameValidation
        )
      },
      csmConfigurationProviderClassName = {
        val className =
          config.getAs[String](csmConfigurationProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          CsmConfigurationProviderClassName,
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderProviderClassName = {
        val className = config.as[String](V1CommonConfigKeys.awsCredentialsProviderProviderClassNameKey)
        ClassCheckUtils.requireClassByName(
          V1CommonConfigKeys.AWSCredentialsProviderProviderClassName,
          className,
          classNameValidation
        )
      },
      awsCredentialsProviderClassName = {
        val className = config.getAs[String](
          V1CommonConfigKeys.awsCredentialsProviderClassNameKey
        )
        ClassCheckUtils.requireClassByName(
          V1CommonConfigKeys.AWSCredentialsProviderClassName,
          className,
          classNameValidation
        )
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
