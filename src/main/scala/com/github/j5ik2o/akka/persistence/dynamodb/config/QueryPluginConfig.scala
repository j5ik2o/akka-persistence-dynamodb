package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.typesafe.config.Config

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import scala.concurrent.duration._

object QueryPluginConfig {

  def fromConfig(config: Config): QueryPluginConfig = {
    QueryPluginConfig(
      tableName = config.asString("table-name", "Journal"),
      tagSeparator = config.asString("tag-separator", ","),
      bufferSize = config.asInt("buffer-size", Int.MaxValue),
      batchSize = config.asInt("batch-size", 16),
      parallelism = config.asInt("parallelism", 32),
      refreshInterval = config.asFiniteDuration("refresh-interval", 1 seconds),
      journalSequenceRetrievalConfig =
        JournalSequenceRetrievalConfig.fromConfig(config.asConfig("journal-sequence-retrieval")),
      clientConfig = DynamoDBClientConfig.fromConfig(config.asConfig("dynamodb-client"))
    )
  }

}
case class QueryPluginConfig(tableName: String,
                             tagSeparator: String,
                             bufferSize: Int,
                             batchSize: Int,
                             parallelism: Int,
                             refreshInterval: FiniteDuration,
                             journalSequenceRetrievalConfig: JournalSequenceRetrievalConfig,
                             clientConfig: DynamoDBClientConfig)
    extends PluginConfig
