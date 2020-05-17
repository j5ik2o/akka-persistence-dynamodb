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

import scala.concurrent.duration._

object QueryPluginConfig extends LoggingSupport {

  val DefaultTableName: String                = JournalPluginConfig.DefaultTableName
  val DefaultTagsIndexName: String            = "TagsIndex"
  val DefaultGetJournalRowsIndexName: String  = JournalPluginConfig.DefaultGetJournalRowsIndexName
  val DefaultTagSeparator: String             = JournalPluginConfig.DefaultTagSeparator
  val DefaultShardCount                       = JournalPluginConfig.DefaultShardCount
  val DefaultRefreshInterval: FiniteDuration  = 1 seconds
  val DefaultMaxBufferSize                    = 500
  val DefaultQueryBatchSize                   = 1024
  val DefaultScanBatchSize                    = 1024
  val DefaultConsistentRead: Boolean          = false
  val DefaultMetricsReporterClassName: String = classOf[NullMetricsReporter].getName

  val legacyConfigLayoutKey       = "legacy-config-layout"
  val tableNameKey                = "table-name"
  val columnsDefKey               = "columns-def"
  val tagsIndexNameKey            = "tags-index-name"
  val getJournalRowsIndexNameKey  = "get-journal-rows-index-name"
  val tagSeparatorKey             = "tag-separator"
  val shardCountKey               = "shard-count"
  val refreshIntervalKey          = "refresh-interval"
  val maxBufferSizeKey            = "max-buffer-size"
  val queryBatchSizeKey           = "query-batch-size"
  val scanBatchSizeKey            = "scan-batch-size"
  val readBackoffKey              = "read-backoff"
  val consistentReadKey           = "consistent-read"
  val journalSequenceRetrievalKey = "journal-sequence-retrieval"
  val metricsReporterClassNameKey = "metrics-reporter-class-name"
  val dynamoDbClientKey           = "dynamo-db-client"

  def fromConfig(config: Config): QueryPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat = config.getOrElse(legacyConfigLayoutKey, default = false)
    val result = QueryPluginConfig(
      legacyConfigFormat,
      tableName = config.getOrElse(tableNameKey, DefaultTableName),
      columnsDefConfig =
        JournalColumnsDefConfig.fromConfig(config.getOrElse[Config](columnsDefKey, ConfigFactory.empty())),
      tagsIndexName = config.getOrElse(tagsIndexNameKey, DefaultTagsIndexName),
      getJournalRowsIndexName = config.getOrElse(getJournalRowsIndexNameKey, DefaultGetJournalRowsIndexName),
      tagSeparator = config.getOrElse(tagSeparatorKey, DefaultTagSeparator),
      shardCount = config.getOrElse(shardCountKey, DefaultShardCount),
      refreshInterval = config.getOrElse(refreshIntervalKey, DefaultRefreshInterval),
      maxBufferSize = config.getOrElse(maxBufferSizeKey, DefaultMaxBufferSize),
      queryBatchSize = config.getOrElse(queryBatchSizeKey, DefaultQueryBatchSize),
      scanBatchSize = config.getOrElse(scanBatchSizeKey, DefaultScanBatchSize),
      readBackoffConfig = BackoffConfig.fromConfig(config.getOrElse[Config](readBackoffKey, ConfigFactory.empty())),
      consistentRead = config.getOrElse(consistentReadKey, DefaultConsistentRead),
      journalSequenceRetrievalConfig = JournalSequenceRetrievalConfig.fromConfig(
        config.getOrElse[Config](journalSequenceRetrievalKey, ConfigFactory.empty())
      ),
      metricsReporterClassName = config.getOrElse(metricsReporterClassNameKey, DefaultMetricsReporterClassName),
      clientConfig = DynamoDBClientConfig
        .fromConfig(config.getOrElse[Config](dynamoDbClientKey, ConfigFactory.empty()), legacyConfigFormat)
    )
    logger.debug("result = {}", result)
    result
  }

}

case class QueryPluginConfig(
    legacyConfigFormat: Boolean,
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
    override val readBackoffConfig: BackoffConfig,
    consistentRead: Boolean,
    journalSequenceRetrievalConfig: JournalSequenceRetrievalConfig,
    metricsReporterClassName: String,
    clientConfig: DynamoDBClientConfig
) extends PluginConfig {
  require(shardCount > 1)
}
