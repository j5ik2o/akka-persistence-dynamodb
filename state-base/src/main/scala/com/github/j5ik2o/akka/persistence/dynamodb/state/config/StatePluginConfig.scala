package com.github.j5ik2o.akka.persistence.dynamodb.state.config
import com.github.j5ik2o.akka.persistence.dynamodb.config.ConfigSupport._
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.state.{
  DynamoDBDurableStateStoreProvider,
  PartitionKeyResolver,
  PartitionKeyResolverProvider,
  TableNameResolver,
  TableNameResolverProvider
}
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }

object StatePluginConfig extends LoggingSupport {

  val DefaultTableName: String                             = "State"
  val DefaultTagSeparator: String                          = ","
  val DefaultShardCount: Int                               = 64
  val DefaultPartitionKeyResolverClassName: String         = classOf[PartitionKeyResolver.Default].getName
  val DefaultPartitionKeyResolverProviderClassName: String = classOf[PartitionKeyResolverProvider.Default].getName
  val DefaultTableNameResolverClassName: String            = classOf[TableNameResolver.Default].getName
  val DefaultTableNameResolverProviderClassName: String    = classOf[TableNameResolverProvider.Default].getName
  val DefaultMetricsReporterClassName: String              = classOf[MetricsReporter.None].getName
  val DefaultMetricsReporterProviderClassName: String      = classOf[MetricsReporterProvider.Default].getName
  val DefaultTraceReporterClassName: String                = classOf[TraceReporter.None].getName
  val DefaultTraceReporterProviderClassName: String        = classOf[TraceReporterProvider.Default].getName

  val tableNameKey                             = "table-name"
  val columnsDefKey                            = "columns-def"
  val tagSeparatorKey                          = "tag-separator"
  val shardCountKey                            = "shard-count"
  val partitionKeyResolverClassNameKey         = "partition-key-resolver-class-name"
  val partitionKeyResolverProviderClassNameKey = "partition-key-resolver-provider-class-name"
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
    val result = new StatePluginConfig(
      sourceConfig = config,
      tableName = config.valueAs(tableNameKey, DefaultTableName),
      columnsDefConfig = StateColumnsDefConfig.fromConfig(config.configAs(columnsDefKey, ConfigFactory.empty())),
      tagSeparator = config.valueAs(tagSeparatorKey, DefaultTagSeparator),
      shardCount = config.valueAs(shardCountKey, DefaultShardCount),
      tableNameResolverClassName = {
        val className = config.valueAs(tableNameResolverClassNameKey, DefaultTableNameResolverClassName)
        ClassCheckUtils.requireClass(classOf[TableNameResolver], className)
      },
      tableNameResolverProviderClassName = {
        val className =
          config.valueAs(tableNameResolverProviderClassNameKey, DefaultTableNameResolverProviderClassName)
        ClassCheckUtils.requireClass(classOf[TableNameResolverProvider], className)
      },
      partitionKeyResolverClassName = {
        val className = config.valueAs(partitionKeyResolverClassNameKey, DefaultPartitionKeyResolverClassName)
        ClassCheckUtils.requireClass(classOf[PartitionKeyResolver], className)
      },
      partitionKeyResolverProviderClassName = {
        val className =
          config.valueAs(partitionKeyResolverProviderClassNameKey, DefaultPartitionKeyResolverProviderClassName)
        ClassCheckUtils.requireClass(classOf[PartitionKeyResolverProvider], className)
      },
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
      writeBackoffConfig = BackoffConfig.fromConfig(config.configAs(writeBackoffKey, ConfigFactory.empty())),
      readBackoffConfig = BackoffConfig.fromConfig(config.configAs(readBackoffKey, ConfigFactory.empty())),
      clientConfig = DynamoDBClientConfig
        .fromConfig(config.configAs(dynamoCbClientKey, ConfigFactory.empty()), legacyConfigFormat = false)
    )
    logger.debug("result = {}", result)
    result
  }
}

final case class StatePluginConfig(
    sourceConfig: Config,
    tableName: String,
    columnsDefConfig: StateColumnsDefConfig,
    tableNameResolverClassName: String,
    tableNameResolverProviderClassName: String,
    tagSeparator: String,
    shardCount: Int,
    partitionKeyResolverClassName: String,
    partitionKeyResolverProviderClassName: String,
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
