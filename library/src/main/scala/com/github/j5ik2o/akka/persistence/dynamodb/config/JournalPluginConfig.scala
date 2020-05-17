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

import akka.stream.OverflowStrategy
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ PartitionKeyResolver, SortKeyResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.NullMetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._

object JournalPluginConfig extends LoggingSupport {

  val DefaultTableName: String                     = "Journal"
  val DefaultShardCount: Int                       = 2
  val DefaultGetJournalRowsIndexName: String       = "GetJournalRowsIndex"
  val DefaultTagSeparator: String                  = ","
  val DefaultPartitionKeyResolverClassName: String = classOf[PartitionKeyResolver.Default].getName
  val DefaultSortKeyResolverClassName: String      = classOf[SortKeyResolver.Default].getName
  val DefaultQueueBufferSize: Int                  = 1024
  val DefaultQueueOverflowStrategy: String         = OverflowStrategy.fail.getClass.getSimpleName
  val DefaultQueueParallelism                      = 1
  val DefaultWriteParallelism                      = 8
  val DefaultQueryBatchSize                        = 512
  val DefaultScanBatchSize                         = 512
  val DefaultReplayBatchSize                       = 512
  val DefaultSoftDeleted                           = true
  val DefaultMetricsReporterClassName: String      = classOf[NullMetricsReporter].getName

  val legacyConfigFormatKey = "legacy-config-format"

  val tableNameKey                     = "table-name"
  val columnsDefKey                    = "columns-def"
  val getJournalRowsIndexNameKey       = "get-journal-rows-index-name"
  val tagSeparatorKey                  = "tag-separator"
  val shardCountKey                    = "shard-count"
  val partitionKeyResolverClassNameKey = "partition-key-resolver-class-name"
  val sortKeyResolverClassNameKey      = "sort-key-resolver-class-name"
  val queueEnableKey                   = "queue-enable"
  val queueBufferSizeKey               = "queue-buffer-size"
  val queueOverflowStrategyKey         = "queue-overflow-strategy"
  val queueParallelismKey              = "queue-parallelism"
  val writeParallelismKey              = "write-parallelism"
  val writeBackoffKey                  = "write-backoff"
  val queryBatchSizeKey                = "query-batch-size"
  val replayBatchSizeKey               = "replay-batch-size"
  val replayBatchRefreshIntervalKey    = "replay-batch-refresh-interval"
  val readBackoffKey                   = "read-backoff"

  def fromConfig(config: Config): JournalPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat = config.getOrElse[Boolean](legacyConfigFormatKey, default = false)
    logger.debug("legacy-config-format = {}", legacyConfigFormat)
    val result = JournalPluginConfig(
      legacyConfigFormat,
      tableName = config.getOrElse[String](tableNameKey, DefaultTableName),
      columnsDefConfig =
        JournalColumnsDefConfig.fromConfig(config.getOrElse[Config](columnsDefKey, ConfigFactory.empty())),
      getJournalRowsIndexName = config.getOrElse[String](getJournalRowsIndexNameKey, DefaultGetJournalRowsIndexName),
      // ---
      tagSeparator = config.getOrElse[String](tagSeparatorKey, DefaultTagSeparator),
      shardCount = config.getOrElse[Int](shardCountKey, DefaultShardCount),
      // ---
      partitionKeyResolverClassName =
        config.getOrElse[String](partitionKeyResolverClassNameKey, DefaultPartitionKeyResolverClassName),
      sortKeyResolverClassName = config.getOrElse[String](sortKeyResolverClassNameKey, DefaultSortKeyResolverClassName),
      // ---
      queueEnable = config.getOrElse[Boolean](queueEnableKey, default = true),
      queueBufferSize = config.getOrElse[Int](queueBufferSizeKey, DefaultQueueBufferSize),
      queueOverflowStrategy = config.getOrElse[String](queueOverflowStrategyKey, DefaultQueueOverflowStrategy),
      queueParallelism = config.getOrElse[Int](queueParallelismKey, DefaultQueueParallelism),
      // ---
      writeParallelism = config.getOrElse[Int](writeParallelismKey, DefaultWriteParallelism),
      writeBackoffConfig = BackoffConfig.fromConfig(config.getOrElse[Config](writeBackoffKey, ConfigFactory.empty())),
      // ---
      queryBatchSize = config.getOrElse[Int](queryBatchSizeKey, DefaultQueryBatchSize),
      replayBatchSize = config.getOrElse[Int](replayBatchSizeKey, DefaultReplayBatchSize),
      replayBatchRefreshInterval = config.getAs[FiniteDuration](replayBatchRefreshIntervalKey),
      readBackoffConfig = BackoffConfig.fromConfig(config.getOrElse[Config](readBackoffKey, ConfigFactory.empty())),
      // ---
      softDeleted = config.getOrElse[Boolean]("soft-delete", DefaultSoftDeleted),
      metricsReporterClassName =
        config.getOrElse[String]("metrics-reporter-class-name", DefaultMetricsReporterClassName),
      clientConfig = DynamoDBClientConfig
        .fromConfig(config.getOrElse[Config]("dynamo-db-client", ConfigFactory.empty()), legacyConfigFormat)
    )
    logger.debug("result = {}", result)
    result
  }

}

trait PluginConfig {
  val tableName: String
  val columnsDefConfig: JournalColumnsDefConfig
  val getJournalRowsIndexName: String
  val queryBatchSize: Int
  val clientConfig: DynamoDBClientConfig
  val readBackoffConfig: BackoffConfig
}

case class JournalPluginConfig(
    legacyConfigFormat: Boolean,
    tableName: String,
    columnsDefConfig: JournalColumnsDefConfig,
    getJournalRowsIndexName: String,
    tagSeparator: String,
    partitionKeyResolverClassName: String,
    sortKeyResolverClassName: String,
    shardCount: Int,
    queueEnable: Boolean,
    queueBufferSize: Int,
    queueOverflowStrategy: String,
    queueParallelism: Int,
    writeParallelism: Int,
    writeBackoffConfig: BackoffConfig,
    queryBatchSize: Int,
    replayBatchSize: Int,
    replayBatchRefreshInterval: Option[FiniteDuration],
    readBackoffConfig: BackoffConfig,
    softDeleted: Boolean,
    metricsReporterClassName: String,
    clientConfig: DynamoDBClientConfig
) extends PluginConfig {
  require(shardCount > 1)
}
