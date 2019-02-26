package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.NotUsed
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.stream.scaladsl.Source

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try

trait JournalDao {

  def delete(persistenceId: String, toSequenceNr: Long): Future[Unit]

  def highestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long]

  def messages(persistenceId: String,
               fromSequenceNr: Long,
               toSequenceNr: Long,
               max: Long): Source[Either[Throwable, PersistentRepr], NotUsed]

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]]

}
