package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow

trait WriteJournalDaoWithUpdates extends WriteJournalDao {

  def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed]

}
