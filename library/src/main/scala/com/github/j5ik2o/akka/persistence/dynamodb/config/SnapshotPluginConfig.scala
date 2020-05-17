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

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.NullMetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._

object SnapshotPluginConfig extends LoggingSupport {

  val legacyConfigLayoutKey       = "legacy-config-layout"
  val tableNameKey                = "table-name"
  val columnsDefKey               = "columns-def"
  val consistentReadKey           = "consistent-read"
  val metricsReporterClassNameKey = "metrics-reporter-class-name"
  val dynamoDbClientKey           = "dynamo-db-client"

  def fromConfig(config: Config): SnapshotPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat = config.getOrElse[Boolean](legacyConfigLayoutKey, default = false)
    val result = SnapshotPluginConfig(
      legacyConfigFormat,
      tableName = config.getOrElse[String](tableNameKey, default = "Snapshot"),
      columnsDefConfig =
        SnapshotColumnsDefConfig.fromConfig(config.getOrElse[Config](columnsDefKey, ConfigFactory.empty())),
      consistentRead = config.getOrElse[Boolean](consistentReadKey, default = false),
      metricsReporterClassName = config.getOrElse[String](
        metricsReporterClassNameKey,
        classOf[NullMetricsReporter].getName
      ),
      clientConfig = DynamoDBClientConfig
        .fromConfig(config.getOrElse[Config](dynamoDbClientKey, ConfigFactory.empty()), legacyConfigFormat)
    )
    logger.debug("result = {}", result)
    result
  }

}

final case class SnapshotPluginConfig(
    legacyConfigFormat: Boolean,
    tableName: String,
    columnsDefConfig: SnapshotColumnsDefConfig,
    consistentRead: Boolean,
    metricsReporterClassName: String,
    clientConfig: DynamoDBClientConfig
)
