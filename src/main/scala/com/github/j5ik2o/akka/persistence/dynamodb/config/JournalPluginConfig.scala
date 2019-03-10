package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

import scala.concurrent.duration._

object JournalPluginConfig {

  def fromConfig(config: Config): JournalPluginConfig = {
    JournalPluginConfig(
      tableName = config.asString("table-name", "Journal"),
      tagSeparator = config.asString("tag-separator", ","),
      bufferSize = config.asInt("buffer-size", Int.MaxValue),
      batchSize = config.asInt("batch-size", 16),
      parallelism = config.asInt("parallelism", 32),
      refreshInterval = config.asFiniteDuration("refresh-interval", 1 seconds),
      clientConfig = DynamoDBClientConfig.fromConfig(config.asConfig("dynamodb-client"))
    )
  }

}

case class JournalPluginConfig(tableName: String,
                               tagSeparator: String,
                               bufferSize: Int,
                               batchSize: Int,
                               parallelism: Int,
                               refreshInterval: FiniteDuration,
                               clientConfig: DynamoDBClientConfig) extends PluginConfig