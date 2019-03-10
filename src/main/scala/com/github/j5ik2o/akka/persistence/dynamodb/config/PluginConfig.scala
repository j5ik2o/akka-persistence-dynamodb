package com.github.j5ik2o.akka.persistence.dynamodb.config

import scala.concurrent.duration.FiniteDuration

trait PluginConfig {
  val tableName: String
  val tagSeparator: String
  val bufferSize: Int
  val batchSize: Int
  val parallelism: Int
  val refreshInterval: FiniteDuration
  val clientConfig: DynamoDBClientConfig
}
