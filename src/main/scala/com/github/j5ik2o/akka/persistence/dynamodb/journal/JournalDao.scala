package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.NotUsed
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.stream.scaladsl.Source
import monix.eval.Task

import scala.collection.immutable
import scala.util.Try

trait JournalDao {

  def delete(persistenceId: String, toSequenceNr: Long): Task[Unit]

  def highestSequenceNr(persistenceId: String, fromSequenceNr: Long): Task[Long]

  def messages(persistenceId: String,
               fromSequenceNr: Long,
               toSequenceNr: Long,
               max: Long): Source[PersistentRepr, NotUsed]

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Task[immutable.Seq[Try[Unit]]]

}
