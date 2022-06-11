package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.journal.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

case class SortKey(value: String) {
  def asString: String = value
}

trait SortKeyResolver {
  def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): SortKey
}

trait SortKeyResolverProvider {

  def create: SortKeyResolver

}

object SortKeyResolverProvider {

  def create(dynamicAccess: DynamicAccess, journalPluginConfig: JournalPluginConfig): SortKeyResolverProvider = {
    val className = journalPluginConfig.sortKeyResolverProviderClassName
    dynamicAccess
      .createInstanceFor[SortKeyResolverProvider](
        className,
        Seq(
          classOf[DynamicAccess]       -> dynamicAccess,
          classOf[JournalPluginConfig] -> journalPluginConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize SortKeyResolverProvider", Some(ex))
    }
  }

  final class Default(
      dynamicAccess: DynamicAccess,
      journalPluginConfig: JournalPluginConfig
  ) extends SortKeyResolverProvider {

    override def create: SortKeyResolver = {
      val className = journalPluginConfig.sortKeyResolverClassName
      val args =
        Seq(classOf[JournalPluginConfig] -> journalPluginConfig)
      dynamicAccess
        .createInstanceFor[SortKeyResolver](
          className,
          args
        ) match {
        case Success(value) => value
        case Failure(ex) =>
          throw new PluginException("Failed to initialize SortKeyResolver", Some(ex))
      }
    }

  }

}

object SortKeyResolver {

  class Default(journalPluginConfig: JournalPluginConfig) extends PersistenceIdWithSeqNr(journalPluginConfig)

  class SeqNr(journalPluginConfig: JournalPluginConfig) extends SortKeyResolver {

    // ${sequenceNumber}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): SortKey = {
      SortKey(sequenceNumber.value.toString)
    }

  }

  class PersistenceIdWithSeqNr(journalPluginConfig: JournalPluginConfig)
      extends SortKeyResolver
      with ToPersistenceIdOps {

    override def separator: String =
      journalPluginConfig.sourceConfig.valueAs[String]("persistence-id-separator", PersistenceId.Separator)

    // ${persistenceId.body}-${sequenceNumber}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): SortKey = {
      val bodyOpt = persistenceId.body
      val seq     = sequenceNumber.value
      val skey = bodyOpt match {
        case Some(pid) =>
          "%s-%019d".format(pid, seq)
        case None => // fallback
          "%019d".format(seq)
      }
      SortKey(skey)
    }

  }

}
