/*
 * Copyright 2022 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.state.config

import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig._
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientVersion, DynamoDBClientConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.state._
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

object StatePluginConfig extends LoggingSupport {

  val tableNameKey                             = "table-name"
  val columnsDefKey                            = "columns-def"
  val tagSeparatorKey                          = "tag-separator"
  val consistentReadKey                        = "consistent-read"
  val shardCountKey                            = "shard-count"
  val partitionKeyResolverClassNameKey         = "partition-key-resolver-class-name"
  val partitionKeyResolverProviderClassNameKey = "partition-key-resolver-provider-class-name"
  val sortKeyResolverClassNameKey              = "sort-key-resolver-class-name"
  val sortKeyResolverProviderClassNameKey      = "sort-key-resolver-provider-class-name"
  val tableNameResolverClassNameKey            = "table-name-resolver-class-name"
  val tableNameResolverProviderClassNameKey    = "table-name-resolver-provider-class-name"
  val writeBackoffKey                          = "write-backoff"
  val readBackoffKey                           = "read-backoff"
  val metricsReporterClassNameKey              = "metrics-reporter-class-name"
  val metricsReporterProviderClassNameKey      = "metrics-reporter-provider-class-name"
  val traceReporterClassNameKey                = "trace-reporter-class-name"
  val traceReporterProviderClassNameKey        = "trace-reporter-provider-class-name"
  val dynamoCbClientKey                        = "dynamo-db-client"

  def fromConfig(config: Config): StatePluginConfig = {
    logger.debug("config = {}", config)
    val clientConfig = DynamoDBClientConfig
      .fromConfig(config.configAs(dynamoCbClientKey, ConfigFactory.empty()), legacyConfigFormat = false)
    val result = new StatePluginConfig(
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
            clientConfig.clientVersion == ClientVersion.V2Dax
          )
      },
      v2DaxSyncClientFactoryClassName = {
        val className = config.value[String](v2DaxSyncClientFactoryClassNameKey)
        ClassCheckUtils
          .requireClassByName(
            V2DaxSyncClientFactoryClassName,
            className,
            clientConfig.clientVersion == ClientVersion.V2Dax
          )
      },
      tableName = config.value[String](tableNameKey),
      columnsDefConfig = StateColumnsDefConfig.fromConfig(config.configAs(columnsDefKey, ConfigFactory.empty())),
      tagSeparator = config.value[String](tagSeparatorKey),
      consistentRead = config.value[Boolean](consistentReadKey),
      shardCount = config.value[Int](shardCountKey),
      tableNameResolverClassName = {
        val className = config.value[String](tableNameResolverClassNameKey)
        ClassCheckUtils.requireClass(classOf[TableNameResolver], className)
      },
      tableNameResolverProviderClassName = {
        val className = config.value[String](tableNameResolverProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[TableNameResolverProvider], className)
      },
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
      metricsReporterClassName = {
        val className = config.valueOptAs[String](metricsReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[MetricsReporter], className)
      },
      metricsReporterProviderClassName = {
        val className = config.value[String](metricsReporterProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[MetricsReporterProvider], className)
      },
      traceReporterProviderClassName = {
        val className = config.value[String](traceReporterProviderClassNameKey)
        ClassCheckUtils.requireClass(classOf[TraceReporterProvider], className)
      },
      traceReporterClassName = {
        val className = config.valueOptAs[String](traceReporterClassNameKey)
        ClassCheckUtils.requireClass(classOf[TraceReporter], className)
      },
      writeBackoffConfig = BackoffConfig.fromConfig(config.configAs(writeBackoffKey, ConfigFactory.empty())),
      readBackoffConfig = BackoffConfig.fromConfig(config.configAs(readBackoffKey, ConfigFactory.empty())),
      clientConfig = clientConfig
    )
    logger.debug("result = {}", result)
    result
  }
}

final case class StatePluginConfig(
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
    columnsDefConfig: StateColumnsDefConfig,
    tableNameResolverClassName: String,
    tableNameResolverProviderClassName: String,
    tagSeparator: String,
    consistentRead: Boolean,
    shardCount: Int,
    partitionKeyResolverClassName: String,
    partitionKeyResolverProviderClassName: String,
    sortKeyResolverClassName: String,
    sortKeyResolverProviderClassName: String,
    metricsReporterProviderClassName: String,
    metricsReporterClassName: Option[String],
    traceReporterProviderClassName: String,
    traceReporterClassName: Option[String],
    writeBackoffConfig: BackoffConfig,
    readBackoffConfig: BackoffConfig,
    clientConfig: DynamoDBClientConfig
) extends PluginConfig {
  override val configRootPath: String = DynamoDBDurableStateStoreProvider.Identifier
}
