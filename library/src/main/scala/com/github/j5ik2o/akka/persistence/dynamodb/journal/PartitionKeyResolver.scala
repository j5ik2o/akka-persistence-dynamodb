package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.text.{ DecimalFormat, NumberFormat }
import java.util.Base64

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

    private val md5 = MessageDigest.getInstance("MD5")
    private val df  = new DecimalFormat("0000000000000000000000000000000000000000")

    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val modelNameOpt = persistenceId.modelName
      val bytes        = persistenceId.asString.reverse.getBytes(StandardCharsets.UTF_8)
      val hash         = BigInt(md5.digest(bytes))
      val mod          = (hash.abs % pluginConfig.shardCount) + 1
      val pkey = modelNameOpt match {
        case Some(modelName) =>
          "%s-%s".format(modelName, df.format(mod))
        case None =>
          df.format(mod)
      }

      PartitionKey(pkey)
    }

  }

}
