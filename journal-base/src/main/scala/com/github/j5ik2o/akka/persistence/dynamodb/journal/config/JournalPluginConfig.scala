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
package com.github.j5ik2o.akka.persistence.dynamodb.journal.config

import akka.stream.OverflowStrategy
import com.github.j5ik2o.akka.persistence.dynamodb.config.BackoffConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig._
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{
  PartitionKeyResolver,
  PartitionKeyResolverProvider,
  SortKeyResolver,
  SortKeyResolverProvider
}
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._

object JournalPluginConfig extends LoggingSupport {

  val legacyConfigFormatKey                    = "legacy-config-format"
  val tableNameKey                             = "table-name"
  val columnsDefKey                            = "columns-def"
  val getJournalRowsIndexNameKey               = "get-journal-rows-index-name"
  val tagSeparatorKey                          = "tag-separator"
  val shardCountKey                            = "shard-count"
  val partitionKeyResolverClassNameKey         = "partition-key-resolver-class-name"
  val partitionKeyResolverProviderClassNameKey = "partition-key-resolver-provider-class-name"
  val sortKeyResolverClassNameKey              = "sort-key-resolver-class-name"
  val sortKeyResolverProviderClassNameKey      = "sort-key-resolver-provider-class-name"
  val queueEnableKey                           = "queue-enable"
  val queueBufferSizeKey                       = "queue-buffer-size"
  val queueOverflowStrategyKey                 = "queue-overflow-strategy"
  val queueParallelismKey                      = "queue-parallelism"
  val writeParallelismKey                      = "write-parallelism"
  val writeBackoffKey                          = "write-backoff"
  val queryBatchSizeKey                        = "query-batch-size"
  val replayBatchSizeKey                       = "replay-batch-size"
  val replayBatchRefreshIntervalKey            = "replay-batch-refresh-interval"
  val readBackoffKey                           = "read-backoff"
  val softDeleteKey                            = "soft-delete"
  val metricsReporterClassNameKey              = "metrics-reporter-class-name"
  val metricsReporterProviderClassNameKey      = "metrics-reporter-provider-class-name"
  val traceReporterClassNameKey                = "trace-reporter-class-name"
  val traceReporterProviderClassNameKey        = "trace-reporter-provider-class-name"
  val dynamoCbClientKey                        = "dynamo-db-client"

  val DefaultLegacyConfigFormat: Boolean                   = false
  val DefaultTableName: String                             = "Journal"
  val DefaultShardCount: Int                               = 64
  val DefaultGetJournalRowsIndexName: String               = "GetJournalRowsIndex"
  val DefaultTagSeparator: String                          = ","
  val DefaultPartitionKeyResolverClassName: String         = classOf[PartitionKeyResolver.Default].getName
  val DefaultPartitionKeyResolverProviderClassName: String = classOf[PartitionKeyResolverProvider.Default].getName
  val DefaultSortKeyResolverClassName: String              = classOf[SortKeyResolver.Default].getName
  val DefaultSortKeyResolverProviderClassName: String      = classOf[SortKeyResolverProvider.Default].getName
  val DefaultQueueEnable: Boolean                          = true
  val DefaultQueueBufferSize: Int                          = 512
  val DefaultQueueOverflowStrategy: String                 = OverflowStrategy.fail.getClass.getSimpleName
  val DefaultQueueParallelism: Int                         = 1
  val DefaultWriteParallelism                              = 16
  val DefaultQueryBatchSize                                = 512
  val DefaultScanBatchSize                                 = 512
  val DefaultReplayBatchSize                               = 512
  val DefaultSoftDeleted                                   = true
  val DefaultMetricsReporterClassName: String              = classOf[MetricsReporter.None].getName
  val DefaultMetricsReporterProviderClassName: String      = classOf[MetricsReporterProvider.Default].getName
  val DefaultTraceReporterClassName: String                = classOf[TraceReporter.None].getName
  val DefaultTraceReporterProviderClassName: String        = classOf[TraceReporterProvider.Default].getName

  def fromConfig(config: Config): JournalPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat = config.valueAs(legacyConfigFormatKey, DefaultLegacyConfigFormat)
    logger.debug("legacy-config-format = {}", legacyConfigFormat)
    val result = JournalPluginConfig(
      legacyConfigFormat,
      sourceConfig = config,
      v1AsyncClientFactoryClassName = {
        config.valueAs(v1AsyncClientFactoryClassNameKey, DefaultV1AsyncClientFactoryClassName)
      },
      v1SyncClientFactoryClassName = {
        config.valueAs(v1SyncClientFactoryClassNameKey, DefaultV1SyncClientFactoryClassName)
      },
      v1DaxAsyncClientFactoryClassName = {
        config.valueAs(v1DaxAsyncClientFactoryClassNameKey, DefaultV1DaxAsyncClientFactoryClassName)
      },
      v1DaxSyncClientFactoryClassName = {
        config.valueAs(v1DaxSyncClientFactoryClassNameKey, DefaultV1DaxSyncClientFactoryClassName)
      },
      v2AsyncClientFactoryClassName = {
        config.valueAs(v2AsyncClientFactoryClassNameKey, DefaultV2AsyncClientFactoryClassName)
      },
      v2SyncClientFactoryClassName = {
        config.valueAs(v2SyncClientFactoryClassNameKey, DefaultV2SyncClientFactoryClassName)
      },
      tableName = config.valueAs(tableNameKey, DefaultTableName),
      columnsDefConfig = JournalColumnsDefConfig.fromConfig(config.configAs(columnsDefKey, ConfigFactory.empty())),
      getJournalRowsIndexName = config
        .valueAs(getJournalRowsIndexNameKey, DefaultGetJournalRowsIndexName),
      // ---
      tagSeparator = config.valueAs(tagSeparatorKey, DefaultTagSeparator),
      shardCount = config.valueAs(shardCountKey, DefaultShardCount),
      // ---
      partitionKeyResolverClassName = {
        val className = config.valueAs(partitionKeyResolverClassNameKey, DefaultPartitionKeyResolverClassName)
        ClassCheckUtils.requireClass(classOf[PartitionKeyResolver], className)
      },
      partitionKeyResolverProviderClassName = {
        val className =
          config.valueAs(partitionKeyResolverProviderClassNameKey, DefaultPartitionKeyResolverProviderClassName)
        ClassCheckUtils.requireClass(classOf[PartitionKeyResolverProvider], className)
      },
      sortKeyResolverClassName = {
        val className = config.valueAs(sortKeyResolverClassNameKey, DefaultSortKeyResolverClassName)
        ClassCheckUtils.requireClass(classOf[SortKeyResolver], className)
      },
      sortKeyResolverProviderClassName = {
        val className =
          config.valueAs(sortKeyResolverProviderClassNameKey, DefaultSortKeyResolverProviderClassName)
        ClassCheckUtils.requireClass(classOf[SortKeyResolverProvider], className)
      },
      // ---
      queueEnable = config.valueAs(queueEnableKey, DefaultQueueEnable),
      queueBufferSize = config.valueAs(queueBufferSizeKey, DefaultQueueBufferSize),
      queueOverflowStrategy = {
        config.valueAs(queueOverflowStrategyKey, DefaultQueueOverflowStrategy).toLowerCase match {
          case s if s == OverflowStrategy.dropHead.getClass.getSimpleName.toLowerCase()   => OverflowStrategy.dropHead
          case s if s == OverflowStrategy.dropTail.getClass.getSimpleName.toLowerCase()   => OverflowStrategy.dropTail
          case s if s == OverflowStrategy.dropBuffer.getClass.getSimpleName.toLowerCase() => OverflowStrategy.dropBuffer
          case s if s == OverflowStrategy.dropNew.getClass.getSimpleName.toLowerCase() =>
            logger.warn("DropNew is not recommended. It may be discontinued in the next version.")
            OverflowStrategy.dropNew
          case s if s == OverflowStrategy.fail.getClass.getSimpleName.toLowerCase() => OverflowStrategy.fail
          case s if s == OverflowStrategy.backpressure.getClass.getSimpleName.toLowerCase() =>
            OverflowStrategy.backpressure
          case _ => throw new IllegalArgumentException("queueOverflowStrategy is invalid")
        }
      },
      queueParallelism = config.valueAs(queueParallelismKey, DefaultQueueParallelism),
      // ---
      writeParallelism = config.valueAs(writeParallelismKey, DefaultWriteParallelism),
      writeBackoffConfig = BackoffConfig.fromConfig(config.configAs(writeBackoffKey, ConfigFactory.empty())),
      // ---
      queryBatchSize = config.valueAs(queryBatchSizeKey, DefaultQueryBatchSize),
      replayBatchSize = config.valueAs(replayBatchSizeKey, DefaultReplayBatchSize),
      replayBatchRefreshInterval = config.valueOptAs[FiniteDuration](replayBatchRefreshIntervalKey),
      readBackoffConfig = BackoffConfig.fromConfig(config.configAs(readBackoffKey, ConfigFactory.empty())),
      // ---
      softDeleted = config.valueAs(softDeleteKey, DefaultSoftDeleted),
      metricsReporterClassName = {
        val className = config.valueOptAs[String](metricsReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[MetricsReporter], className)
      },
      metricsReporterProviderClassName = {
        val className =
          config.valueAs(metricsReporterProviderClassNameKey, DefaultMetricsReporterProviderClassName)
        ClassCheckUtils.requireClass(classOf[MetricsReporterProvider], className)
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
        .fromConfig(config.configAs(dynamoCbClientKey, ConfigFactory.empty()), legacyConfigFormat),
      journalRowDriverWrapperClassName = {
        val className = config.valueOptAs[String]("journal-row-driver-wrapper-class-name")
        ClassCheckUtils.requireClass(classOf[JournalRowWriteDriver], className)
      }
    )
    logger.debug("result = {}", result)
    result
  }

}

final case class JournalPluginConfig(
    legacyConfigFormat: Boolean,
    sourceConfig: Config,
    v1AsyncClientFactoryClassName: String,
    v1SyncClientFactoryClassName: String,
    v1DaxAsyncClientFactoryClassName: String,
    v1DaxSyncClientFactoryClassName: String,
    v2AsyncClientFactoryClassName: String,
    v2SyncClientFactoryClassName: String,
    tableName: String,
    columnsDefConfig: JournalColumnsDefConfig,
    getJournalRowsIndexName: String,
    tagSeparator: String,
    partitionKeyResolverClassName: String,
    sortKeyResolverClassName: String,
    partitionKeyResolverProviderClassName: String,
    sortKeyResolverProviderClassName: String,
    shardCount: Int,
    queueEnable: Boolean,
    queueBufferSize: Int,
    queueOverflowStrategy: OverflowStrategy,
    queueParallelism: Int,
    writeParallelism: Int,
    writeBackoffConfig: BackoffConfig,
    queryBatchSize: Int,
    replayBatchSize: Int,
    replayBatchRefreshInterval: Option[FiniteDuration],
    readBackoffConfig: BackoffConfig,
    softDeleted: Boolean,
    metricsReporterProviderClassName: String,
    metricsReporterClassName: Option[String],
    traceReporterProviderClassName: String,
    traceReporterClassName: Option[String],
    clientConfig: DynamoDBClientConfig,
    journalRowDriverWrapperClassName: Option[String]
) extends JournalPluginBaseConfig {
  require(shardCount > 1)
  override val configRootPath: String = "j5ik2o.dynamo-db-journal"
}
