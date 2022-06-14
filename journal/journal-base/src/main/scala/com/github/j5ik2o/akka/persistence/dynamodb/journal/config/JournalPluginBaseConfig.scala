package com.github.j5ik2o.akka.persistence.dynamodb.journal.config

import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }

trait JournalPluginBaseConfig extends PluginConfig {
  val columnsDefConfig: JournalColumnsDefConfig
  val getJournalRowsIndexName: String
  val queryBatchSize: Int
  val readBackoffConfig: BackoffConfig
}
