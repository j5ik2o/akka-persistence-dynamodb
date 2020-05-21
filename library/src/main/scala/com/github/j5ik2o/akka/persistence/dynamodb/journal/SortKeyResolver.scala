package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import net.ceedubs.ficus.Ficus._

import scala.collection.immutable.Seq

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
      ).getOrElse(
        throw new ClassNotFoundException(className)
      )
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
        ).getOrElse(throw new ClassNotFoundException(className))
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
      journalPluginConfig.sourceConfig.getOrElse[String]("separator", PersistenceId.Separator)

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
