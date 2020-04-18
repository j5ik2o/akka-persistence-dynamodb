package com.github.j5ik2o.akka.persistence.dynamodb.journal

import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.typesafe.config.Config

case class PartitionKey(private val value: String) {
  def asString: String = value
}

trait PartitionKeyResolver {
  def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey
}

object PartitionKeyResolver {

  class Default(config: Config) extends PartitionKeyResolver {

    private val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey =
      PartitionKey(s"${persistenceId.asString}-${sequenceNumber.value % pluginConfig.shardCount}")

  }

}
