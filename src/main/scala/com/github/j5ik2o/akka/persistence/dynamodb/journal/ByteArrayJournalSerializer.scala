package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.persistence.PersistentRepr
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer

class ByteArrayJournalSerializer(serialization: Serialization, separator: String)
    extends FlowPersistentReprSerializer[JournalRow] {

  override def serialize(persistentRepr: PersistentRepr, tags: Set[String]): Either[Throwable, JournalRow] = {
    serialization
      .serialize(persistentRepr)
      .map(
        JournalRow(Long.MinValue,
                   persistentRepr.deleted,
                   persistentRepr.persistenceId,
                   persistentRepr.sequenceNr,
                   _,
                   encodeTags(tags, separator))
      ).toEither
  }

  override def deserialize(journalRow: JournalRow): Either[Throwable, (PersistentRepr, Set[String], Long)] = {
    serialization
      .deserialize(journalRow.message, classOf[PersistentRepr])
      .map((_, decodeTags(journalRow.tags, separator), journalRow.ordering)).toEither
  }
}
