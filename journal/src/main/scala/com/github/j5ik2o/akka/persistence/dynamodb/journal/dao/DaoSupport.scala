package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao

import akka.persistence.PersistentRepr
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import akka.{ actor, NotUsed }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.DaoSupport.{
  Continue,
  ContinueDelayed,
  FlowControl,
  Stop
}
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object DaoSupport {
  private sealed trait FlowControl

  /** Keep querying - used when we are sure that there is more events to fetch */
  private case object Continue extends FlowControl

  /** Keep querying with delay - used when we have consumed all events, but want to poll for future events
    */
  private case object ContinueDelayed extends FlowControl

  /** Stop querying - used when we reach the desired offset */
  private case object Stop extends FlowControl
}

trait DaoSupport {

  protected def serializer: FlowPersistentReprSerializer[JournalRow]
  protected def metricsReporter: Option[MetricsReporter]

  protected def journalRowDriver: JournalRowReadDriver

  implicit def ec: ExecutionContext
  implicit def mat: Materializer

  def getMessagesAsJournalRow(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[JournalRow, NotUsed] =
    journalRowDriver.getJournalRows(persistenceId, fromSequenceNr, toSequenceNr, max, deleted)

  def getMessagesAsPersistentRepr(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[Try[PersistentRepr], NotUsed] = {
    getMessagesAsJournalRow(persistenceId, fromSequenceNr, toSequenceNr, max, deleted)
      .via(serializer.deserializeFlowWithoutTagsAsTry)
  }

  def getMessagesAsPersistentReprWithBatch(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      batchSize: Int,
      refreshInterval: Option[(FiniteDuration, actor.Scheduler)]
  ): Source[Try[PersistentRepr], NotUsed] = {
    Source
      .unfoldAsync[(Long, FlowControl), scala.collection.immutable.Seq[Try[PersistentRepr]]](
        (Math.max(1, fromSequenceNr), Continue)
      ) { case (from, control) =>
        def retrieveNextBatch()
            : Future[Option[((Long, FlowControl), scala.collection.immutable.Seq[Try[PersistentRepr]])]] = {
          for {
            xs <- getMessagesAsPersistentRepr(
              PersistenceId(persistenceId),
              SequenceNumber(from),
              SequenceNumber(toSequenceNr),
              batchSize
            ).runWith(Sink.seq)
          } yield {
            val hasMoreEvents = xs.size == batchSize
            // Events are ordered by sequence number, therefore the last one is the largest)
            val lastSeqNrInBatch: Option[Long] = xs.lastOption match {
              case Some(Success(repr)) => Some(repr.sequenceNr)
              case Some(Failure(e))    => throw e // fail the returned Future
              case None                => None
            }
            val hasLastEvent = lastSeqNrInBatch.exists(_ >= toSequenceNr)
            val nextControl: FlowControl =
              if (hasLastEvent || from > toSequenceNr) Stop
              else if (hasMoreEvents) Continue
              else if (refreshInterval.isEmpty) Stop
              else ContinueDelayed

            val nextFrom: Long = lastSeqNrInBatch match {
              // Continue querying from the last sequence number (the events are ordered)
              case Some(lastSeqNr) => lastSeqNr + 1
              case None            => from
            }
            Some((nextFrom, nextControl), xs)
          }
        }
        control match {
          case Stop     => Future.successful(None)
          case Continue => retrieveNextBatch()
          case ContinueDelayed =>
            val (delay, scheduler) = refreshInterval.get
            akka.pattern.after(delay, scheduler)(retrieveNextBatch())
        }
      }
      .mapConcat(identity)
  }

}
