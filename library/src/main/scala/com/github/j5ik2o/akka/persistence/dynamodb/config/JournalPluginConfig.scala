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
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ PartitionKeyResolver, SortKeyResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.NullMetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

import scala.concurrent.duration._

object JournalPluginConfig {

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
  val DefaultConsistentRead                        = false
  val DefaultSoftDeleted                           = true
  val DefaultMetricsReporterClassName: String      = classOf[NullMetricsReporter].getName

  def fromConfig(config: Config): JournalPluginConfig = {
    JournalPluginConfig(
      tableName = config.asString("table-name", default = DefaultTableName),
      columnsDefConfig = JournalColumnsDefConfig.fromConfig(config.asConfig("columns-def")),
      getJournalRowsIndexName =
        config.asString("get-journal-rows-index-name", default = DefaultGetJournalRowsIndexName),
      tagSeparator = config.asString("tag-separator", default = DefaultTagSeparator),
      shardCount = config.asInt("shard-count", default = DefaultShardCount),
      partitionKeyResolverClassName =
        config.asString("partition-key-resolver-class-name", default = DefaultPartitionKeyResolverClassName),
      sortKeyResolverClassName =
        config.asString("sort-key-resolver-class-name", default = DefaultSortKeyResolverClassName),
      queueEnable = config.asBoolean("queue-enable", true),
      queueBufferSize = config.asInt("queue-buffer-size", default = DefaultQueueBufferSize),
      queueOverflowStrategy = config.asString("queue-overflow-strategy", DefaultQueueOverflowStrategy),
      queueParallelism = config.asInt("queue-parallelism", default = DefaultQueueParallelism),
      writeParallelism = config.asInt("write-parallelism", default = DefaultWriteParallelism),
      writeMinBackoff = config.asFiniteDuration("write-min-backoff", 3 seconds),
      writeMaxBackoff = config.asFiniteDuration("write-max-backoff", 15 seconds),
      writeBackoffRandomFactor = 0.8,
      queryBatchSize = config.asInt("query-batch-size", default = DefaultQueryBatchSize),
      scanBatchSize = config.asInt("scan-batch-size", default = DefaultScanBatchSize),
      replayBatchSize = config.asInt("replay-batch-size", default = DefaultReplayBatchSize),
      consistentRead = config.asBoolean("consistent-read", default = DefaultConsistentRead),
      softDeleted = config.asBoolean("soft-delete", default = DefaultSoftDeleted),
      metricsReporterClassName = config.asString("metrics-reporter-class-name", DefaultMetricsReporterClassName),
      clientConfig = DynamoDBClientConfig.fromConfig(config.asConfig("dynamo-db-client"))
    )
  }

}

case class JournalPluginConfig(
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
    writeMinBackoff: FiniteDuration,
    writeMaxBackoff: FiniteDuration,
    writeBackoffRandomFactor: Double,
    queryBatchSize: Int,
    scanBatchSize: Int,
    replayBatchSize: Int,
    consistentRead: Boolean,
    softDeleted: Boolean,
    metricsReporterClassName: String,
    clientConfig: DynamoDBClientConfig
) {
  require(shardCount > 1)
}
