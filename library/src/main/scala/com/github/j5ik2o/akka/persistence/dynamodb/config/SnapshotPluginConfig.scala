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

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

object SnapshotPluginConfig {

  def fromConfig(config: Config): SnapshotPluginConfig = {
    SnapshotPluginConfig(
      tableName = config.asString("table-name", default = "Snapshot"),
      columnsDefConfig = SnapshotColumnsDefConfig.fromConfig(config.asConfig("columns-def")),
      consistentRead = config.asBoolean("consistent-read", default = false),
      metricsReporterClassName = config.asString(
        "metrics-reporter-class-name",
        "com.github.j5ik2o.akka.persistence.dynamodb.metrics.NullMetricsReporter"
      ),
      clientConfig = DynamoDBClientConfig
        .fromConfig(config.asConfig("dynamo-db-client"), config.asBoolean("legacy-config-layout", false))
    )
  }

}

final case class SnapshotPluginConfig(
    tableName: String,
    columnsDefConfig: SnapshotColumnsDefConfig,
    consistentRead: Boolean,
    metricsReporterClassName: String,
    clientConfig: DynamoDBClientConfig
)
