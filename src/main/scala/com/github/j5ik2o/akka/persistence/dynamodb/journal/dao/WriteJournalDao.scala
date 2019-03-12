package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow

trait WriteJournalDao {
  import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ PersistenceId, SequenceNumber }

  def deleteMessages(persistenceId: PersistenceId, toSequenceNr: SequenceNumber): Source[Long, NotUsed]

  def highestSequenceNr(persistenceId: PersistenceId, fromSequenceNr: SequenceNumber): Source[Long, NotUsed]

  def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed]

  def getMessages(persistenceId: PersistenceId,
                  fromSequenceNr: SequenceNumber,
                  toSequenceNr: SequenceNumber,
                  max: Long,
                  deleted: Option[Boolean] = Some(false)): Source[JournalRow, NotUsed]

  def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed]

}
