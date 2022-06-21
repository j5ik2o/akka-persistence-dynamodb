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
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{
  ClientVersion,
  CommonConfigKeys,
  DynamoDBClientConfig
}
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
  val dynamoCbClientKey                        = "dynamo-db-client"
  val journalRowDriverWrapperClassNameKey      = "journal-row-driver-wrapper-class-name"

  def fromConfig(config: Config): JournalPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat = config.value[Boolean](legacyConfigFormatKey)
    val clientConfig =
      DynamoDBClientConfig.fromConfig(config.configAs(dynamoCbClientKey, ConfigFactory.empty()), legacyConfigFormat)
    logger.debug("legacy-config-format = {}", legacyConfigFormat)
    val result = JournalPluginConfig(
      legacyConfigFormat,
      sourceConfig = config,
      v1AsyncClientFactoryClassName = {
        val className = config.value[String](v1AsyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(V1AsyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V1)
      },
      v1SyncClientFactoryClassName = {
        val className = config.value[String](v1SyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(V1SyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V1)
      },
      v1DaxAsyncClientFactoryClassName = {
        val className = config.value[String](v1DaxAsyncClientFactoryClassNameKey)
        ClassCheckUtils.requireClassByName(
          V1DaxAsyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V1Dax
        )
      },
      v1DaxSyncClientFactoryClassName = {
        val className = config.value[String](v1DaxSyncClientFactoryClassNameKey)
        ClassCheckUtils.requireClassByName(
          V1DaxSyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V1Dax
        )
      },
      v2AsyncClientFactoryClassName = {
        val className = config.value[String](v2AsyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(V2AsyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V2)
      },
      v2SyncClientFactoryClassName = {
        val className = config.value[String](v2SyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(V2SyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V2)
      },
      v2DaxAsyncClientFactoryClassName = {
        val className = config.value[String](v2DaxAsyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(
            V2DaxAsyncClientFactoryClassName,
            className,
            clientConfig.clientVersion == ClientVersion.V2
          )
      },
      v2DaxSyncClientFactoryClassName = {
        val className = config.value[String](v2DaxSyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(
            V2DaxSyncClientFactoryClassName,
            className,
            clientConfig.clientVersion == ClientVersion.V2
          )
      },
      tableName = config.value[String](tableNameKey),
      columnsDefConfig = JournalColumnsDefConfig.fromConfig(
        config.configAs(columnsDefKey, ConfigFactory.empty())
      ),
      getJournalRowsIndexName = config.value[String](getJournalRowsIndexNameKey),
      // ---
      tagSeparator = config.value[String](tagSeparatorKey),
      shardCount = config.value[Int](shardCountKey),
      // ---
      partitionKeyResolverClassName = {
        val className = config.value[String](partitionKeyResolverClassNameKey)
        ClassCheckUtils.requireClass(classOf[PartitionKeyResolver], className)
      },
      partitionKeyResolverProviderClassName = {
        val className = config.value[String](partitionKeyResolverProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[PartitionKeyResolverProvider], className)
      },
      sortKeyResolverClassName = {
        val className = config.value[String](sortKeyResolverClassNameKey)
        ClassCheckUtils.requireClass(classOf[SortKeyResolver], className)
      },
      sortKeyResolverProviderClassName = {
        val className = config.value[String](sortKeyResolverProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[SortKeyResolverProvider], className)
      },
      // ---
      queueEnable = config.value[Boolean](queueEnableKey),
      queueBufferSize = config.value[Int](queueBufferSizeKey),
      queueOverflowStrategy = {
        config.value[String](queueOverflowStrategyKey).toLowerCase match {
          case s if s == OverflowStrategy.dropHead.getClass.getSimpleName.toLowerCase() =>
            OverflowStrategy.dropHead
          case s if s == OverflowStrategy.dropTail.getClass.getSimpleName.toLowerCase() =>
            OverflowStrategy.dropTail
          case s if s == OverflowStrategy.dropBuffer.getClass.getSimpleName.toLowerCase() =>
            OverflowStrategy.dropBuffer
          case s if s == OverflowStrategy.dropNew.getClass.getSimpleName.toLowerCase() =>
            logger.warn(
              "DropNew is not recommended. It may be discontinued in the next version."
            )
            OverflowStrategy.dropNew
          case s if s == OverflowStrategy.fail.getClass.getSimpleName.toLowerCase() =>
            OverflowStrategy.fail
          case s if s == OverflowStrategy.backpressure.getClass.getSimpleName.toLowerCase() =>
            OverflowStrategy.backpressure
          case _ =>
            throw new IllegalArgumentException(
              "queueOverflowStrategy is invalid"
            )
        }
      },
      queueParallelism = config.value[Int](queueParallelismKey),
      // ---
      writeParallelism = config.value[Int](writeParallelismKey),
      writeBackoffConfig = BackoffConfig.fromConfig(config.configAs(writeBackoffKey, ConfigFactory.empty())),
      // ---
      queryBatchSize = config.value[Int](queryBatchSizeKey),
      replayBatchSize = config.value[Int](replayBatchSizeKey),
      replayBatchRefreshInterval = config.valueOptAs[FiniteDuration](replayBatchRefreshIntervalKey),
      readBackoffConfig = BackoffConfig.fromConfig(config.configAs(readBackoffKey, ConfigFactory.empty())),
      // ---
      softDeleted = config.value[Boolean](softDeleteKey),
      metricsReporterClassName = {
        val className = config.valueOptAs[String](CommonConfigKeys.metricsReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[MetricsReporter], className)
      },
      metricsReporterProviderClassName = {
        val className = config.value[String](CommonConfigKeys.metricsReporterProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[MetricsReporterProvider], className)
      },
      traceReporterProviderClassName = {
        val className = config.value[String](CommonConfigKeys.traceReporterProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[TraceReporterProvider], className)
      },
      traceReporterClassName = {
        val className = config.valueOptAs[String](CommonConfigKeys.traceReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[TraceReporter], className)
      },
      clientConfig = clientConfig,
      journalRowDriverWrapperClassName = {
        val className = config.valueOptAs[String](journalRowDriverWrapperClassNameKey)
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
    v2DaxAsyncClientFactoryClassName: String,
    v2DaxSyncClientFactoryClassName: String,
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
