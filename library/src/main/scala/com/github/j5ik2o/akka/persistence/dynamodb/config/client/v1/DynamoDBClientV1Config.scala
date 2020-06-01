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

import com.amazonaws.handlers.RequestHandler2
import com.amazonaws.metrics.RequestMetricCollector
import com.amazonaws.monitoring.MonitoringListener
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._

import scala.collection.immutable._

object DynamoDBClientV1Config extends LoggingSupport {

  val dispatcherNameKey                          = "dispatcher-name"
  val clientConfigurationKey                     = "client-configuration"
  val requestMetricCollectorProviderClassNameKey = "request-metric-collector-provider-class-name"
  val requestMetricCollectorClassNameKey         = "request-metric-collector-class-name"
  val monitoringListenerProviderClassNameKey     = "monitoring-listener-provider-class-name"
  val monitoringListenerClassNameKey             = "monitoring-listener-class-name"
  val requestHandlersProviderClassNameKey        = "request-handlers-provider-class-name"
  val requestHandlerClassNamesKey                = "request-handler-class-names"

  val DefaultRequestMetricCollectorProviderClassName: String = classOf[RequestMetricCollectorProvider.Default].getName
  val DefaultMonitoringListenerProviderClassName: String     = classOf[MonitoringListenerProvider.Default].getName

  def fromConfig(config: Config): DynamoDBClientV1Config = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV1Config(
      sourceConfig = config,
      dispatcherName = config.getAs[String](dispatcherNameKey),
      clientConfiguration =
        ClientConfiguration.fromConfig(config.getOrElse[Config](clientConfigurationKey, ConfigFactory.empty())),
      requestMetricCollectorProviderClassName = {
        val className =
          config.getOrElse(requestMetricCollectorProviderClassNameKey, DefaultRequestMetricCollectorProviderClassName)
        ClassCheckUtils.requireClass(classOf[RequestMetricCollectorProvider], className)
      },
      requestMetricCollectorClassName = {
        val className = config.getAs[String](requestMetricCollectorClassNameKey)
        ClassCheckUtils.requireClass(classOf[RequestMetricCollector], className)
      },
      monitoringListenerProviderClassName = {
        val className = config
          .getOrElse(monitoringListenerProviderClassNameKey, DefaultMonitoringListenerProviderClassName)
        ClassCheckUtils.requireClass(classOf[MonitoringListenerProvider], className)
      },
      monitoringListenerClassName = {
        val className = config.getAs[String](monitoringListenerClassNameKey)
        ClassCheckUtils.requireClass(classOf[MonitoringListener], className)
      },
      requestHandlersProviderClassName = {
        val className = config
          .getOrElse[String](requestHandlersProviderClassNameKey, classOf[RequestHandlersProvider.Default].getName)
        ClassCheckUtils.requireClass(classOf[RequestHandlersProvider], className)
      },
      requestHandlerClassNames = {
        val classNames = config.getOrElse[Seq[String]](requestHandlerClassNamesKey, Seq.empty)
        classNames.map { className => ClassCheckUtils.requireClass(classOf[RequestHandler2], className) }
      }
    )
    logger.debug("result = {}", result)
    result
  }
}

case class DynamoDBClientV1Config(
    sourceConfig: Config,
    dispatcherName: Option[String],
    clientConfiguration: ClientConfiguration,
    requestMetricCollectorProviderClassName: String,
    requestMetricCollectorClassName: Option[String],
    monitoringListenerProviderClassName: String,
    monitoringListenerClassName: Option[String],
    requestHandlersProviderClassName: String,
    requestHandlerClassNames: Seq[String]
)
