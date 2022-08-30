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
import net.ceedubs.ficus.Ficus._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

object SnapshotPluginConfig extends LoggingSupport {

  val legacyConfigFormatKey = "legacy-config-format"
  val legacyTableFormatKey  = "legacy-table-format"
  val tableNameKey          = "table-name"
  val columnsDefKey         = "columns-def"
  val consistentReadKey     = "consistent-read"

  val getSnapshotRowsIndexNameKey = "get-snapshot-rows-index-name"

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

  def fromConfig(config: Config): SnapshotPluginConfig = {
    logger.debug("config = {}", config)
    val legacyConfigFormat = config.as[Boolean](legacyConfigFormatKey)
    logger.debug("legacy-config-format = {}", legacyConfigFormat)
    val legacyTableFormat = config.as[Boolean](legacyTableFormatKey)
    logger.debug("legacy-table-format = {}", legacyTableFormat)
    val clientConfig = DynamoDBClientConfig
      .fromConfig(
        config.getAs[Config](dynamoDbClientKey).getOrElse(ConfigFactory.empty()),
        legacyConfigFormat
      )
    val result = SnapshotPluginConfig(
      sourceConfig = config,
      v1AsyncClientFactoryClassName = {
        val className = config.as[String](v1AsyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(V1AsyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V1)
      },
      v1SyncClientFactoryClassName = {
        val className = config.as[String](v1SyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(V1SyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V1)
      },
      v1DaxAsyncClientFactoryClassName = {
        val className = config.as[String](v1DaxAsyncClientFactoryClassNameKey)
        ClassCheckUtils.requireClassByName(
          V1DaxAsyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V1Dax
        )
      },
      v1DaxSyncClientFactoryClassName = {
        val className = config.as[String](v1DaxSyncClientFactoryClassNameKey)
        ClassCheckUtils.requireClassByName(
          V1DaxSyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V1Dax
        )
      },
      v2AsyncClientFactoryClassName = {
        val className = config.as[String](v2AsyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(V2AsyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V2)
      },
      v2SyncClientFactoryClassName = {
        val className = config.as[String](v2SyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(V2SyncClientFactoryClassName, className, clientConfig.clientVersion == ClientVersion.V2)
      },
      v2DaxAsyncClientFactoryClassName = {
        val className = config.as[String](v2DaxAsyncClientFactoryClassNameKey)
        ClassCheckUtils.requireClassByName(
          V2DaxAsyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V2
        )
      },
      v2DaxSyncClientFactoryClassName = {
        val className = config.as[String](v2DaxSyncClientFactoryClassNameKey)
        ClassCheckUtils.requireClassByName(
          V2DaxSyncClientFactoryClassName,
          className,
          clientConfig.clientVersion == ClientVersion.V2
        )
      },
      legacyConfigFormat = legacyConfigFormat,
      legacyTableFormat = legacyTableFormat,
      tableName = config.as[String](tableNameKey),
      columnsDefConfig =
        SnapshotColumnsDefConfig.fromConfig(config.getAs[Config](columnsDefKey).getOrElse(ConfigFactory.empty())),
      getSnapshotRowsIndexName = config.as[String](getSnapshotRowsIndexNameKey),
      consistentRead = config.as[Boolean](consistentReadKey),
      shardCount = config.as[Int](shardCountKey),
      // ---
      partitionKeyResolverClassName = {
        val className = config.as[String](partitionKeyResolverClassNameKey)
        ClassCheckUtils.requireClass(classOf[PartitionKeyResolver], className)
      },
      partitionKeyResolverProviderClassName = {
        val className = config.as[String](partitionKeyResolverProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[PartitionKeyResolverProvider], className)
      },
      sortKeyResolverClassName = {
        val className = config.as[String](sortKeyResolverClassNameKey)
        ClassCheckUtils.requireClass(classOf[SortKeyResolver], className)
      },
      sortKeyResolverProviderClassName = {
        val className = config.as[String](sortKeyResolverProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[SortKeyResolverProvider], className)
      },
      // ---
      metricsReporterProviderClassName = {
        val className = config.as[String](CommonConfigKeys.metricsReporterProviderClassNameKey)
        ClassCheckUtils
          .requireClass(classOf[MetricsReporterProvider], className)
      },
      metricsReporterClassName = {
        val className = config.getAs[String](CommonConfigKeys.metricsReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[MetricsReporter], className)
      },
      traceReporterProviderClassName = {
        val className = config.as[String](CommonConfigKeys.traceReporterProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[TraceReporterProvider], className)
      },
      traceReporterClassName = {
        val className = config.getAs[String](CommonConfigKeys.traceReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[TraceReporter], className)
      },
      writeBackoffConfig = BackoffConfig.fromConfig(
        config.getAs[Config](writeBackoffKey).getOrElse(ConfigFactory.empty())
      ),
      readBackoffConfig = BackoffConfig.fromConfig(
        config.getAs[Config](readBackoffKey).getOrElse(ConfigFactory.empty())
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
