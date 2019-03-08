package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.NotUsed
import akka.stream.scaladsl.Source

trait WriteJournalDao {

  def deleteMessages(persistenceId: String, toSequenceNr: Long): Source[Unit, NotUsed]

  def highestSequenceNr(persistenceId: String, fromSequenceNr: Long): Source[Long, NotUsed]

  def getMessages(persistenceId: String,
                  fromSequenceNr: Long,
                  toSequenceNr: Long,
                  max: Long,
                  deleted: Option[Boolean] = Some(false)): Source[JournalRow, NotUsed]

  def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed]

}
