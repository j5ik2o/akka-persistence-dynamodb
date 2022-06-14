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
package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.config

import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig._
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{
  ClientVersion,
  CommonConfigKeys,
  DynamoDBClientConfig
}
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.{
  PartitionKeyResolver,
  PartitionKeyResolverProvider,
  SortKeyResolver,
  SortKeyResolverProvider
}
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

object SnapshotPluginConfig extends LoggingSupport {

  val legacyConfigFormatKey = "legacy-config-format"
  val legacyTableFormatKey  = "legacy-table-format"
  val tableNameKey          = "table-name"
  val columnsDefKey         = "columns-def"
  val consistentReadKey     = "consistent-read"

  val getSnapshotRowsIndexNameKey = "get-snapshot-rows-index"

  val dynamoDbClientKey = "dynamo-db-client"
  val writeBackoffKey   = "write-backoff"
  val readBackoffKey    = "read-backoff"

  val shardCountKey                    = "shard-count"
  val partitionKeyResolverClassNameKey = "partition-key-resolver-class-name"
  val partitionKeyResolverProviderClassNameKey =
    "partition-key-resolver-provider-class-name"
  val sortKeyResolverClassNameKey = "sort-key-resolver-class-name"
  val sortKeyResolverProviderClassNameKey =
    "sort-key-resolver-provider-class-name"

  val DefaultLegacyConfigFormat: Boolean                   = false
  val DefaultLegacyTableFormat: Boolean                    = false
  val DefaultLegacyConfigLayoutKey: Boolean                = false
  val DefaultTableName: String                             = "Snapshot"
  val DefaultGetSnapshotRowsIndexName                      = "GetSnapshotRowsIndex"
  val DefaultConsistentRead: Boolean                       = false
  val DefaultShardCount: Int                               = 64
  val DefaultPartitionKeyResolverClassName: String         = classOf[PartitionKeyResolver.Default].getName
  val DefaultPartitionKeyResolverProviderClassName: String = classOf[PartitionKeyResolverProvider.Default].getName
  val DefaultSortKeyResolverClassName: String              = classOf[SortKeyResolver.Default].getName
  val DefaultSortKeyResolverProviderClassName: String      = classOf[SortKeyResolverProvider.Default].getName

  val DefaultMetricsReporterClassName: String         = classOf[MetricsReporter.None].getName
  val DefaultMetricsReporterProviderClassName: String = classOf[MetricsReporterProvider.Default].getName
  val DefaultTraceReporterClassName: String           = classOf[TraceReporter.None].getName
  val DefaultTraceReporterProviderClassName: String   = classOf[TraceReporterProvider.Default].getName

  def fromConfig(config: Config): SnapshotPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat =
      config.valueAs(legacyConfigFormatKey, DefaultLegacyConfigFormat)
    logger.debug("legacy-config-format = {}", legacyConfigFormat)
    val legacyTableFormat =
      config.valueAs(legacyTableFormatKey, DefaultLegacyTableFormat)
    logger.debug("legacy-table-format = {}", legacyTableFormat)
    val clientConfig = DynamoDBClientConfig
      .fromConfig(
        config.configAs(dynamoDbClientKey, ConfigFactory.empty()),
        legacyConfigFormat
      )
    val result = SnapshotPluginConfig(
      sourceConfig = config,
      v1AsyncClientFactoryClassName = {
        val className = config.valueAs(v1AsyncClientFactoryClassNameKey, DefaultV1AsyncClientFactoryClassName)
        ClassCheckUtils
          .requireClassByName(V1AsyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V1)
      },
      v1SyncClientFactoryClassName = {
        val className = config.valueAs(v1SyncClientFactoryClassNameKey, DefaultV1SyncClientFactoryClassName)
        ClassCheckUtils
          .requireClassByName(V1SyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V1)
      },
      v1DaxAsyncClientFactoryClassName = {
        val className = config.valueAs(v1DaxAsyncClientFactoryClassNameKey, DefaultV1DaxAsyncClientFactoryClassName)
        ClassCheckUtils.requireClassByName(
          V1DaxAsyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V1Dax
        )
      },
      v1DaxSyncClientFactoryClassName = {
        val className = config.valueAs(v1DaxSyncClientFactoryClassNameKey, DefaultV1DaxSyncClientFactoryClassName)
        ClassCheckUtils.requireClassByName(
          V1DaxSyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V1Dax
        )
      },
      v2AsyncClientFactoryClassName = {
        val className = config.valueAs(v2AsyncClientFactoryClassNameKey, DefaultV2AsyncClientFactoryClassName)
        ClassCheckUtils
          .requireClassByName(V2AsyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V2)
      },
      v2SyncClientFactoryClassName = {
        val className = config.valueAs(v2SyncClientFactoryClassNameKey, DefaultV2SyncClientFactoryClassName)
        ClassCheckUtils
          .requireClassByName(V2SyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V2)
      },
      v2DaxAsyncClientFactoryClassName = {
        val className = config.valueAs(v2DaxAsyncClientFactoryClassNameKey, DefaultV2DaxAsyncClientFactoryClassName)
        ClassCheckUtils.requireClassByName(
          V2DaxAsyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V2
        )
      },
      v2DaxSyncClientFactoryClassName = {
        val className = config.valueAs(v2DaxSyncClientFactoryClassNameKey, DefaultV2DaxSyncClientFactoryClassName)
        ClassCheckUtils.requireClassByName(
          V2DaxSyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V2
        )
      },
      legacyConfigFormat = legacyConfigFormat,
      legacyTableFormat = legacyTableFormat,
      tableName = config.valueAs(tableNameKey, DefaultTableName),
      columnsDefConfig = SnapshotColumnsDefConfig.fromConfig(config.configAs(columnsDefKey, ConfigFactory.empty())),
      getSnapshotRowsIndexName = config.valueAs(getSnapshotRowsIndexNameKey, DefaultGetSnapshotRowsIndexName),
      consistentRead = config.valueAs(consistentReadKey, DefaultConsistentRead),
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
        val className = config.valueAs(sortKeyResolverProviderClassNameKey, DefaultSortKeyResolverProviderClassName)
        ClassCheckUtils.requireClass(classOf[SortKeyResolverProvider], className)
      },
      // ---
      metricsReporterProviderClassName = {
        val className =
          config.valueAs(CommonConfigKeys.metricsReporterProviderClassNameKey, DefaultMetricsReporterProviderClassName)
        ClassCheckUtils
          .requireClass(classOf[MetricsReporterProvider], className)
      },
      metricsReporterClassName = {
        val className = config.valueOptAs[String](CommonConfigKeys.metricsReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[MetricsReporter], className)
      },
      traceReporterProviderClassName = {
        val className =
          config.valueAs(CommonConfigKeys.traceReporterProviderClassNameKey, DefaultTraceReporterProviderClassName)
        ClassCheckUtils.requireClass(classOf[TraceReporterProvider], className)
      },
      traceReporterClassName = {
        val className = config.valueOptAs[String](CommonConfigKeys.traceReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[TraceReporter], className)
      },
      writeBackoffConfig = BackoffConfig.fromConfig(
        config.configAs(writeBackoffKey, ConfigFactory.empty())
      ),
      readBackoffConfig = BackoffConfig.fromConfig(
        config.configAs(readBackoffKey, ConfigFactory.empty())
      ),
      clientConfig = clientConfig
    )
    logger.debug("result = {}", result)
    result
  }

}

final case class SnapshotPluginConfig(
    sourceConfig: Config,
    v1AsyncClientFactoryClassName: String,
    v1SyncClientFactoryClassName: String,
    v1DaxAsyncClientFactoryClassName: String,
    v1DaxSyncClientFactoryClassName: String,
    v2AsyncClientFactoryClassName: String,
    v2SyncClientFactoryClassName: String,
    v2DaxAsyncClientFactoryClassName: String,
    v2DaxSyncClientFactoryClassName: String,
    legacyConfigFormat: Boolean,
    legacyTableFormat: Boolean,
    tableName: String,
    columnsDefConfig: SnapshotColumnsDefConfig,
    getSnapshotRowsIndexName: String,
    consistentRead: Boolean,
    partitionKeyResolverClassName: String,
    sortKeyResolverClassName: String,
    partitionKeyResolverProviderClassName: String,
    sortKeyResolverProviderClassName: String,
    shardCount: Int,
    metricsReporterProviderClassName: String,
    metricsReporterClassName: Option[String],
    writeBackoffConfig: BackoffConfig,
    traceReporterProviderClassName: String,
    traceReporterClassName: Option[String],
    readBackoffConfig: BackoffConfig,
    clientConfig: DynamoDBClientConfig
) extends PluginConfig {
  override val configRootPath: String = "j5ik2o.dynamo-db-snapshot"
}
