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

object JournalPluginConfig {

  val DefaultShardCount = 2

  def fromConfig(config: Config): JournalPluginConfig = {
    JournalPluginConfig(
      tableName = config.asString("table-name", default = "Journal"),
      columnsDefConfig = JournalColumnsDefConfig.fromConfig(config.asConfig("columns-def")),
      getJournalRowsIndexName = config.asString("get-journal-rows-index-name", default = "GetJournalRowsIndex"),
      tagSeparator = config.asString("tag-separator", default = ","),
      shardCount = config.asInt("shard-count", default = DefaultShardCount),
      partitionKeyResolverClassName =
        config.asString("partition-key-resolver-class-name", default = classOf[PartitionKeyResolver.Default].getName),
      sortKeyResolverClassName =
        config.asString("sort-key-resolver-class-name", default = classOf[SortKeyResolver.Default].getName),
      queueBufferSize = config.asInt("queue-buffer-size", default = 1024),
      queueOverflowStrategy = config.asString("queue-overflow-strategy", OverflowStrategy.fail.getClass.getSimpleName),
      queueParallelism = config.asInt("queue-parallelism", default = 1),
      writeParallelism = config.asInt("write-parallelism", default = 8),
      queryBatchSize = config.asInt("query-batch-size", default = 512),
      scanBatchSize = config.asInt("scan-batch-size", default = 512),
      replayBatchSize = config.asInt("replay-batch-size", default = 512),
      consistentRead = config.asBoolean("consistent-read", default = false),
      softDeleted = config.asBoolean("soft-delete", default = true),
      metricsReporterClassName = config.asString(
        "metrics-reporter-class-name",
        classOf[NullMetricsReporter].getName
      ),
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
    queueBufferSize: Int,
    queueOverflowStrategy: String,
    queueParallelism: Int,
    writeParallelism: Int,
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
