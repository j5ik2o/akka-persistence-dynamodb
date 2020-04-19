package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.util.Base64

import com.typesafe.config.Config

case class SortKey(value: String) {
  def asString: String = value
}

trait SortKeyResolver {
  def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): SortKey
}

object SortKeyResolver {

  class Default(config: Config) extends PersistenceIdWithSeqNr(config)

  class SeqNr(config: Config) extends SortKeyResolver {

    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): SortKey = {
      SortKey(sequenceNumber.value.toString)
    }

  }

  class PersistenceIdWithSeqNr(config: Config) extends SortKeyResolver {

    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): SortKey = {
      val pidOpt = persistenceId.id
      val seq    = sequenceNumber.value
      val skey = pidOpt match {
        case Some(pid) =>
          "%s-%019d".format(pid, seq)
        case None =>
          "%019d".format(seq)
      }
      SortKey(skey)
    }
  }

}
