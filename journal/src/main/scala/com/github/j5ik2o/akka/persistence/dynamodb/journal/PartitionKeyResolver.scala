package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.ConfigSupport._
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.text.DecimalFormat
import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

case class PartitionKey(private val value: String) {
  def asString: String = value
}

trait PartitionKeyResolver {

  def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey

}

trait PartitionKeyResolverProvider {

  def create: PartitionKeyResolver

}

object PartitionKeyResolverProvider {

  def create(dynamicAccess: DynamicAccess, journalPluginConfig: JournalPluginConfig): PartitionKeyResolverProvider = {
    val className = journalPluginConfig.partitionKeyResolverProviderClassName
    dynamicAccess
      .createInstanceFor[PartitionKeyResolverProvider](
        className,
        Seq(
          classOf[DynamicAccess]       -> dynamicAccess,
          classOf[JournalPluginConfig] -> journalPluginConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize PartitionKeyResolverProvider", Some(ex))
    }

  }

  final class Default(dynamicAccess: DynamicAccess, journalPluginConfig: JournalPluginConfig)
      extends PartitionKeyResolverProvider {

    override def create: PartitionKeyResolver = {
      val className = journalPluginConfig.partitionKeyResolverClassName
      val args =
        Seq(classOf[JournalPluginConfig] -> journalPluginConfig)
      dynamicAccess
        .createInstanceFor[PartitionKeyResolver](
          className,
          args
        ) match {
        case Success(value) => value
        case Failure(ex) =>
          throw new PluginException("Failed to initialize PartitionKeyResolver", Some(ex))
      }
    }

  }
}

object PartitionKeyResolver {

  class Default(journalPluginConfig: JournalPluginConfig) extends SequenceNumberBased(journalPluginConfig)

  class SequenceNumberBased(journalPluginConfig: JournalPluginConfig) extends PartitionKeyResolver {

    // ${persistenceId}-${sequenceNumber % shardCount}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val pkey = s"${persistenceId.asString}-${sequenceNumber.value % journalPluginConfig.shardCount}"
      PartitionKey(pkey)
    }

  }

  class PersistenceIdBased(journalPluginConfig: JournalPluginConfig)
      extends PartitionKeyResolver
      with ToPersistenceIdOps {

    override def separator: String =
      journalPluginConfig.sourceConfig.valueAs[String]("persistence-id-separator", PersistenceId.Separator)

    // ${persistenceId.prefix}-${md5(persistenceId.reverse) % shardCount}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val md5          = MessageDigest.getInstance("MD5")
      val df           = new DecimalFormat("0000000000000000000000000000000000000000")
      val bytes        = persistenceId.asString.reverse.getBytes(StandardCharsets.UTF_8)
      val hash         = BigInt(md5.digest(bytes))
      val mod          = hash.abs % journalPluginConfig.shardCount + 1
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
