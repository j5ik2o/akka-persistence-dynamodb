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

import scala.concurrent.duration._

object JournalPluginConfig {

  def fromConfig(config: Config): JournalPluginConfig = {
    JournalPluginConfig(
      tableName = config.asString("table-name", default = "Journal"),
      columnsDefConfig = JournalColumnsDefConfig.fromConfig(config.asConfig("columns-def")),
      getJournalRowsIndexName = config.asString("get-journal-rows-index-name", default = "GetJournalRowsIndex"),
      tagSeparator = config.asString("tag-separator", default = ","),
      bufferSize = config.asInt("buffer-size", default = Int.MaxValue),
      queueParallelism = config.asInt("queue-parallelism", default = 256),
      writeParallelism = config.asInt("write-parallelism", default = 256),
      refreshInterval = config.asFiniteDuration("refresh-interval", default = 1 seconds),
      softDeleted = config.asBoolean("soft-delete", default = true),
      shardCount = config.asInt("shard-count", default = 64),
      metricsReporterClassName = config.asString(
        "metrics-reporter-class-name",
        "com.github.j5ik2o.akka.persistence.dynamodb.metrics.NullMetricsReporter"
      ),
      clientConfig = DynamoDBClientConfig.fromConfig(config.asConfig("dynamodb-client"))
    )
  }

}

case class JournalPluginConfig(
    tableName: String,
    columnsDefConfig: JournalColumnsDefConfig,
    getJournalRowsIndexName: String,
    tagSeparator: String,
    bufferSize: Int,
    queueParallelism: Int,
    writeParallelism: Int,
    refreshInterval: FiniteDuration,
    softDeleted: Boolean,
    shardCount: Int,
    metricsReporterClassName: String,
    clientConfig: DynamoDBClientConfig
)
