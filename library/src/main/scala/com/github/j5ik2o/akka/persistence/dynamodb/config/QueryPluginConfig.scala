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

import com.github.j5ik2o.akka.persistence.dynamodb.metrics.NullMetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

import scala.concurrent.duration._

object QueryPluginConfig {

  val DefaultTableName: String                = JournalPluginConfig.DefaultTableName
  val DefaultTagsIndexName: String            = "TagsIndex"
  val DefaultGetJournalRowsIndexName: String  = JournalPluginConfig.DefaultGetJournalRowsIndexName
  val DefaultTagSeparator: String             = JournalPluginConfig.DefaultTagSeparator
  val DefaultRefreshInterval: FiniteDuration  = 1 seconds
  val DefaultMaxBufferSize                    = 500
  val DefaultQueryBatchSize                   = 1024
  val DefaultScanBatchSize                    = 1024
  val DefaultConsistentRead: Boolean          = JournalPluginConfig.DefaultConsistentRead
  val DefaultMetricsReporterClassName: String = classOf[NullMetricsReporter].getName

  def fromConfig(config: Config): QueryPluginConfig = {
    QueryPluginConfig(
      tableName = config.asString("table-name", default = DefaultTableName),
      columnsDefConfig = JournalColumnsDefConfig.fromConfig(config.asConfig("columns-def")),
      tagsIndexName = config.asString("tags-index-name", default = DefaultTagsIndexName),
      getJournalRowsIndexName =
        config.asString(key = "get-journal-rows-index-name", default = DefaultGetJournalRowsIndexName),
      tagSeparator = config.asString("tag-separator", default = DefaultTagSeparator),
      shardCount = config.asInt("shard-count", default = JournalPluginConfig.DefaultShardCount),
      refreshInterval = config.asFiniteDuration("refresh-interval", default = DefaultRefreshInterval),
      maxBufferSize = config.asInt("max-buffer-size", default = DefaultMaxBufferSize),
      queryBatchSize = config.asInt("query-batch-size", default = DefaultQueryBatchSize),
      scanBatchSize = config.asInt("scan-batch-size", default = DefaultScanBatchSize),
      consistentRead = config.asBoolean("consistent-read", default = DefaultConsistentRead),
      journalSequenceRetrievalConfig =
        JournalSequenceRetrievalConfig.fromConfig(config.asConfig("journal-sequence-retrieval")),
      metricsReporterClassName =
        config.asString("metrics-reporter-class-name", default = DefaultMetricsReporterClassName),
      clientConfig = DynamoDBClientConfig.fromConfig(config.asConfig("dynamo-db-client"))
    )
  }

}

case class QueryPluginConfig(
    tableName: String,
    columnsDefConfig: JournalColumnsDefConfig,
    tagsIndexName: String,
    getJournalRowsIndexName: String,
    tagSeparator: String,
    refreshInterval: FiniteDuration,
    shardCount: Int,
    maxBufferSize: Int,
    queryBatchSize: Int,
    scanBatchSize: Int,
    consistentRead: Boolean,
    journalSequenceRetrievalConfig: JournalSequenceRetrievalConfig,
    metricsReporterClassName: String,
    clientConfig: DynamoDBClientConfig
) {
  require(shardCount > 1)
}
