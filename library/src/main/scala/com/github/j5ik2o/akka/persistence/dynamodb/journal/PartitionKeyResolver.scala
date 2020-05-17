package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.text.DecimalFormat

import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

case class PartitionKey(private val value: String) {
  def asString: String = value
}

trait PartitionKeyResolver {

  def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey

}

object PartitionKeyResolver {

  class Default(config: Config) extends SequenceNumberBased(config)

  class SequenceNumberBased(config: Config) extends PartitionKeyResolver {

    private val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

    // ${persistenceId}-${sequenceNumber % shardCount}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val pkey = s"${persistenceId.asString}-${sequenceNumber.value % pluginConfig.shardCount}"
      PartitionKey(pkey)
    }

  }

  class PersistenceIdBased(config: Config) extends PartitionKeyResolver with ToPersistenceIdOps {

    private val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

    private val md5 = MessageDigest.getInstance("MD5")
    private val df  = new DecimalFormat("0000000000000000000000000000000000000000")

    override def separator: String = config.asString("persistence-id-separator", PersistenceId.Separator)

    // ${persistenceId.prefix}-${md5(persistenceId.reverse) % shardCount}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val bytes        = persistenceId.asString.reverse.getBytes(StandardCharsets.UTF_8)
      val hash         = BigInt(md5.digest(bytes))
      val mod          = (hash.abs % pluginConfig.shardCount) + 1
      val modelNameOpt = persistenceId.prefix
      val pkey = modelNameOpt match {
        case Some(modelName) =>
          "%s-%s".format(modelName, df.format(mod))
        case None => // fallback
          df.format(mod)
      }
      PartitionKey(pkey)
    }

  }

}
