package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao

import akka.NotUsed
import akka.actor.Scheduler
import akka.persistence.PersistentRepr
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait WriteJournalDao extends JournalDaoWithReadMessages {

  def deleteMessages(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber
  ): Source[Long, NotUsed]

  def highestSequenceNr(persistenceId: PersistenceId, fromSequenceNr: SequenceNumber): Source[Long, NotUsed]

  def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed]

  def dispose(): Unit

}

trait JournalDaoWithUpdates extends WriteJournalDao {

  def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed]

}

trait JournalDaoWithReadMessages {

  def getMessagesAsPersistentRepr(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[Try[PersistentRepr], NotUsed]

  def getMessagesAsPersistentReprWithBatch(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      batchSize: Int,
      refreshInterval: Option[(FiniteDuration, Scheduler)]
  ): Source[Try[PersistentRepr], NotUsed]

}
