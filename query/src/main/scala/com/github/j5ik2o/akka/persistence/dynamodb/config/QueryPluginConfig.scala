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
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._

object QueryPluginConfig extends LoggingSupport {

  val legacyConfigFormatKey               = "legacy-config-format"
  val tableNameKey                        = "table-name"
  val columnsDefKey                       = "columns-def"
  val tagsIndexNameKey                    = "tags-index-name"
  val getJournalRowsIndexNameKey          = "get-journal-rows-index-name"
  val tagSeparatorKey                     = "tag-separator"
  val shardCountKey                       = "shard-count"
  val refreshIntervalKey                  = "refresh-interval"
  val maxBufferSizeKey                    = "max-buffer-size"
  val queryBatchSizeKey                   = "query-batch-size"
  val scanBatchSizeKey                    = "scan-batch-size"
  val readBackoffKey                      = "read-backoff"
  val consistentReadKey                   = "consistent-read"
  val journalSequenceRetrievalKey         = "journal-sequence-retrieval"
  val metricsReporterClassNameKey         = "metrics-reporter-class-name"
  val metricsReporterProviderClassNameKey = "metrics-reporter-provider-class-name"
  val traceReporterClassNameKey           = "trace-reporter-class-name"
  val traceReporterProviderClassNameKey   = "trace-reporter-provider-class-name"
  val dynamoDbClientKey                   = "dynamo-db-client"

  val DefaultLegacyConfigFormat: Boolean              = false
  val DefaultTableName: String                        = JournalPluginConfig.DefaultTableName
  val DefaultTagsIndexName: String                    = "TagsIndex"
  val DefaultGetJournalRowsIndexName: String          = JournalPluginConfig.DefaultGetJournalRowsIndexName
  val DefaultTagSeparator: String                     = JournalPluginConfig.DefaultTagSeparator
  val DefaultShardCount: Int                          = JournalPluginConfig.DefaultShardCount
  val DefaultRefreshInterval: FiniteDuration          = 1.seconds
  val DefaultMaxBufferSize: Int                       = 500
  val DefaultQueryBatchSize: Int                      = 1024
  val DefaultScanBatchSize: Int                       = 1024
  val DefaultConsistentRead: Boolean                  = false
  val DefaultMetricsReporterClassName: String         = classOf[MetricsReporter.None].getName
  val DefaultMetricsReporterProviderClassName: String = classOf[MetricsReporterProvider.Default].getName
  val DefaultTraceReporterClassName: String           = classOf[TraceReporter.None].getName
  val DefaultTraceReporterProviderClassName: String   = classOf[TraceReporterProvider.Default].getName

  def fromConfig(config: Config): QueryPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat = config.valueAs(legacyConfigFormatKey, DefaultLegacyConfigFormat)
    logger.debug("legacy-config-format = {}", legacyConfigFormat)
    val result = QueryPluginConfig(
      sourceConfig = config,
      legacyConfigFormat,
      tableName = config.valueAs(tableNameKey, DefaultTableName),
      columnsDefConfig = JournalColumnsDefConfig.fromConfig(config.configAs(columnsDefKey, ConfigFactory.empty())),
      getJournalRowsIndexName = config.valueAs(getJournalRowsIndexNameKey, DefaultGetJournalRowsIndexName),
      tagsIndexName = config.valueAs(tagsIndexNameKey, DefaultTagsIndexName),
      tagSeparator = config.valueAs(tagSeparatorKey, DefaultTagSeparator),
      shardCount = config.valueAs(shardCountKey, DefaultShardCount),
      refreshInterval = config.valueAs(refreshIntervalKey, DefaultRefreshInterval),
      maxBufferSize = config.valueAs(maxBufferSizeKey, DefaultMaxBufferSize),
      queryBatchSize = config.valueAs(queryBatchSizeKey, DefaultQueryBatchSize),
      scanBatchSize = config.valueAs(scanBatchSizeKey, DefaultScanBatchSize),
      readBackoffConfig = BackoffConfig.fromConfig(config.configAs(readBackoffKey, ConfigFactory.empty())),
      consistentRead = config.valueAs(consistentReadKey, DefaultConsistentRead),
      journalSequenceRetrievalConfig = JournalSequenceRetrievalConfig.fromConfig(
        config.configAs(journalSequenceRetrievalKey, ConfigFactory.empty())
      ),
      metricsReporterProviderClassName = {
        val className = config.valueAs(metricsReporterProviderClassNameKey, DefaultMetricsReporterProviderClassName)
        ClassCheckUtils.requireClass(classOf[MetricsReporterProvider], className)
      },
      metricsReporterClassName = {
        val className = config.valueOptAs(metricsReporterClassNameKey) // DefaultMetricsReporterClassName)
        ClassCheckUtils.requireClass(classOf[MetricsReporter], className)
      },
      traceReporterProviderClassName = {
        val className =
          config.valueAs(traceReporterProviderClassNameKey, DefaultTraceReporterProviderClassName)
        ClassCheckUtils.requireClass(classOf[TraceReporterProvider], className)
      },
      traceReporterClassName = {
        val className = config.valueOptAs[String](traceReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[TraceReporter], className)
      },
      clientConfig = DynamoDBClientConfig
        .fromConfig(config.configAs(dynamoDbClientKey, ConfigFactory.empty()), legacyConfigFormat)
    )
    logger.debug("result = {}", result)
    result
  }

}

case class QueryPluginConfig(
    sourceConfig: Config,
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
    metricsReporterProviderClassName: String,
    metricsReporterClassName: Option[String],
    traceReporterProviderClassName: String,
    traceReporterClassName: Option[String],
    clientConfig: DynamoDBClientConfig
) extends JournalPluginBaseConfig {
  require(shardCount > 1)
  override val configRootPath: String = "j5ik2o.dynamo-db-read-journal"
}
