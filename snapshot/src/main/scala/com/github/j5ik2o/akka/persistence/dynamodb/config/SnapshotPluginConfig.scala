/*
 * Copyright 2019 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.config.ConfigSupport._
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

object SnapshotPluginConfig extends LoggingSupport {

  val legacyConfigFormatKey               = "legacy-config-format"
  val tableNameKey                        = "table-name"
  val columnsDefKey                       = "columns-def"
  val consistentReadKey                   = "consistent-read"
  val metricsReporterClassNameKey         = "metrics-reporter-class-name"
  val metricsReporterProviderClassNameKey = "metrics-reporter-provider-class-name"
  val dynamoDbClientKey                   = "dynamo-db-client"
  val writeBackoffKey                     = "write-backoff"
  val readBackoffKey                      = "read-backoff"

  val DefaultLegacyConfigFormat: Boolean              = false
  val DefaultLegacyConfigLayoutKey: Boolean           = false
  val DefaultTableName: String                        = "Snapshot"
  val DefaultConsistentRead: Boolean                  = false
  val DefaultMetricsReporterClassName: String         = classOf[MetricsReporter.None].getName
  val DefaultMetricsReporterProviderClassName: String = classOf[MetricsReporterProvider.Default].getName

  def fromConfig(config: Config): SnapshotPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat = config.valueAs(legacyConfigFormatKey, DefaultLegacyConfigFormat)
    logger.debug("legacy-config-format = {}", legacyConfigFormat)
    val result = SnapshotPluginConfig(
      sourceConfig = config,
      legacyConfigFormat,
      tableName = config.valueAs(tableNameKey, DefaultTableName),
      columnsDefConfig = SnapshotColumnsDefConfig.fromConfig(config.configAs(columnsDefKey, ConfigFactory.empty())),
      consistentRead = config.valueAs(consistentReadKey, DefaultConsistentRead),
      metricsReporterProviderClassName = {
        val className =
          config.valueAs(metricsReporterProviderClassNameKey, DefaultMetricsReporterProviderClassName)
        ClassCheckUtils.requireClass(classOf[MetricsReporterProvider], className)
      },
      metricsReporterClassName = {
        val className = config.valueOptAs[String](metricsReporterClassNameKey) // , DefaultMetricsReporterClassName)
        ClassCheckUtils.requireClass(classOf[MetricsReporter], className)
      },
      writeBackoffConfig = BackoffConfig.fromConfig(config.configAs(writeBackoffKey, ConfigFactory.empty())),
      readBackoffConfig = BackoffConfig.fromConfig(config.configAs(readBackoffKey, ConfigFactory.empty())),
      clientConfig = DynamoDBClientConfig
        .fromConfig(config.configAs(dynamoDbClientKey, ConfigFactory.empty()), legacyConfigFormat)
    )
    logger.debug("result = {}", result)
    result
  }

}

final case class SnapshotPluginConfig(
    sourceConfig: Config,
    legacyConfigFormat: Boolean,
    tableName: String,
    columnsDefConfig: SnapshotColumnsDefConfig,
    consistentRead: Boolean,
    metricsReporterProviderClassName: String,
    metricsReporterClassName: Option[String],
    writeBackoffConfig: BackoffConfig,
    readBackoffConfig: BackoffConfig,
    clientConfig: DynamoDBClientConfig
) extends PluginConfig {
  override val configRootPath: String = "j5ik2o.dynamo-db-snapshot"
}
