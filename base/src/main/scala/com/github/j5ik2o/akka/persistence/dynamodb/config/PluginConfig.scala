package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

trait PluginConfig {
  val configRootPath: String
  val tableName: String
  val metricsReporterProviderClassName: String
  val metricsReporterClassName: Option[String]
  val traceReporterProviderClassName: String
  val traceReporterClassName: Option[String]
  val clientConfig: DynamoDBClientConfig
}
