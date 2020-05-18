package com.github.j5ik2o.akka.persistence.dynamodb.journal

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

case class SortKey(value: String) {
  def asString: String = value
}

trait SortKeyResolver {
  def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): SortKey
}

object SortKeyResolver {

  class Default(config: Config) extends PersistenceIdWithSeqNr(config)

  class SeqNr(config: Config) extends SortKeyResolver {

    // ${sequenceNumber}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): SortKey = {
      SortKey(sequenceNumber.value.toString)
    }

  }

  class PersistenceIdWithSeqNr(config: Config) extends SortKeyResolver with ToPersistenceIdOps {

    override def separator: String = config.getOrElse[String]("separator", PersistenceId.Separator)

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
