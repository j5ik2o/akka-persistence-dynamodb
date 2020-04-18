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

  class Default(config: Config) extends Mod(config)

  class Random(config: Config) extends PartitionKeyResolver {

    private val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val pkey = s"${persistenceId.asString}-${sequenceNumber.value % pluginConfig.shardCount}"
      PartitionKey(pkey)
    }

  }

  class Mod(config: Config) extends PartitionKeyResolver {

    private val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

    private def abs(n: Long): Long = {
      if (n == Long.MinValue) 0L else math.abs(n)
    }

    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val modelNameOpt = persistenceId.modelName
      val hash         = abs(persistenceId.asString.##)
      val mod          = hash % pluginConfig.shardCount
      val pkey         = modelNameOpt.fold(mod.toString)(v => s"$v-$mod")
      PartitionKey(pkey)
    }

  }

}
