package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.JournalRow

import scala.concurrent.Future

trait ReadJournalDao {

  /**
    * Returns distinct stream of persistenceIds
    */
  def allPersistenceIdsSource(max: Long): Source[String, NotUsed]

  /**
    * Returns a Source of deserialized data for certain tag from an offset. The result is sorted by
    * the global ordering of the events.
    * Each element with be a try with a PersistentRepr, set of tags, and a Long representing the global ordering of events
    */
  def eventsByTag(tag: String, offset: Long, maxOffset: Long, max: Long): Source[JournalRow, NotUsed]

  /**
    * Returns a Source of bytes for a certain persistenceId
    */
  def getMessages(persistenceId: String,
                  fromSequenceNr: Long,
                  toSequenceNr: Long,
                  max: Long,
                  deleted: Option[Boolean] = Some(false)): Source[JournalRow, NotUsed]

  /**
    * @param offset Minimum value to retrieve
    * @param limit Maximum number of values to retrieve
    * @return A Source of journal event sequence numbers (corresponding to the Ordering column)
    */
  def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed]

  /**
    * @return The value of the maximum (ordering) id in the journal
    */
  def maxJournalSequence(): Source[Long, NotUsed]
}
