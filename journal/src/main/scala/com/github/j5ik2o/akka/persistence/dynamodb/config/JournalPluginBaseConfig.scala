package com.github.j5ik2o.akka.persistence.dynamodb.config

trait JournalPluginBaseConfig extends PluginConfig {
  val columnsDefConfig: JournalColumnsDefConfig
  val getJournalRowsIndexName: String
  val queryBatchSize: Int
  val readBackoffConfig: BackoffConfig
}
