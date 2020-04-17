/*
 * Copyright 2017 Dennis Vriend
 * Copyright 2019 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.query

import akka.actor.{ Actor, ActorLogging, Props, Status, Timers }
import akka.pattern._
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalSequenceRetrievalConfig
import com.github.j5ik2o.akka.persistence.dynamodb.query.dao.ReadJournalDao

import scala.concurrent.duration.FiniteDuration
import scala.util.control.Breaks

object JournalSequenceActor {

  def props(readJournalDao: ReadJournalDao, config: JournalSequenceRetrievalConfig)(
      implicit materializer: Materializer
  ): Props = Props(new JournalSequenceActor(readJournalDao, config))

  private case object QueryOrderingIds

  private case class NewOrderingIds(originalOffset: Long, elements: Seq[OrderingId])

  private case class ScheduleAssumeMaxOrderingId(max: OrderingId)

  private case class AssumeMaxOrderingId(max: OrderingId)

  case object GetMaxOrderingId

  case class MaxOrderingId(maxOrdering: OrderingId)

  private case object QueryOrderingIdsTimerKey

  private case object AssumeMaxOrderingIdTimerKey

  private type OrderingId = Long

  case class Range[T: Numeric](from: T, until: T) {
    private val ev = implicitly[Numeric[T]]

    def contain(value: T): Boolean =
      if (ev.lteq(from, value) && ev.gt(value, until)) true else false

    def isEmpty: Boolean = ev.lteq(until, from)

    def nonEmpty: Boolean = !isEmpty
  }

  /**
    * Efficient representation of missing elements using NumericRanges.
    * It can be seen as a collection of OrderingIds
    */
  private case class MissingElements(elements: Seq[Range[OrderingId]]) {

    def addRange(from: OrderingId, until: OrderingId): MissingElements = {
      val newRange = Range(from, until)
      MissingElements(elements :+ newRange)
    }

    def contains(id: OrderingId): Boolean = elements.exists(_.contain(id))

    def isEmpty: Boolean = {
      val b      = new Breaks
      var result = true
      b.breakable {
        val itr = elements.toIterator
        while (itr.hasNext) {
          val v = itr.next()
          if (v.nonEmpty) {
            result = false
            b.break()
          }
        }
      }
      result
    }
  }

  private object MissingElements {
    def empty: MissingElements = MissingElements(Vector.empty)
  }

  implicit private class IntOps(val init: OrderingId) extends AnyVal {

    def untilWithForall(until: OrderingId, f: OrderingId => Boolean): Boolean = {
      val b      = new Breaks
      var result = true
      var index  = init
      b.breakable {
        while (index < until) {
          if (!f(index)) {
            result = false
            b.break()
          }
          index += 1
        }
      }
      result
    }
  }

}

/**
  * To support the EventsByTag query, this actor keeps track of which rows are visible in the database.
  * This is required to guarantee the EventByTag does not skip any rows in case rows with a higher (ordering) id are
  * visible in the database before rows with a lower (ordering) id.
  */
class JournalSequenceActor(readJournalDao: ReadJournalDao, config: JournalSequenceRetrievalConfig)(
    implicit materializer: Materializer
) extends Actor
    with ActorLogging
    with Timers {

  import JournalSequenceActor._
  import config.{ batchSize, maxBackoffQueryDelay, maxTries, queryDelay }
  import context.dispatcher

  override def receive: Receive = receive(0L, Map.empty, 0)

  override def preStart(): Unit = {
    self ! QueryOrderingIds
    readJournalDao.maxJournalSequence().runWith(Sink.head).onComplete {
      case scala.util.Success(maxInDatabase) =>
        self ! ScheduleAssumeMaxOrderingId(maxInDatabase)
      case scala.util.Failure(t) =>
        log.info("Failed to recover fast, using event-by-event recovery instead. Cause: {}", t)
    }
  }

  /**
    * @param currentMaxOrdering The highest ordering value for which it is known that no missing elements exist
    * @param missingByCounter   A map with missing orderingIds. The key of the map is the count at which the missing elements
    *                           can be assumed to be "skipped ids" (they are no longer assumed missing).
    * @param moduloCounter      A counter which is incremented every time a new query have been executed, modulo `maxTries`
    * @param previousDelay      The last used delay (may change in case failures occur)
    */
  def receive(
      currentMaxOrdering: OrderingId,
      missingByCounter: Map[Int, MissingElements],
      moduloCounter: Int,
      previousDelay: FiniteDuration = queryDelay
  ): Receive = {

    case ScheduleAssumeMaxOrderingId(max) =>
      // All elements smaller than max can be assumed missing after this delay
      val delay = queryDelay * maxTries
      timers.startSingleTimer(key = AssumeMaxOrderingIdTimerKey, AssumeMaxOrderingId(max), delay)

    case AssumeMaxOrderingId(max) =>
      if (currentMaxOrdering < max) {
        context.become(receive(max, missingByCounter, moduloCounter, previousDelay))
      }

    case GetMaxOrderingId =>
      sender() ! MaxOrderingId(currentMaxOrdering)

    case QueryOrderingIds =>
      readJournalDao
        .journalSequence(currentMaxOrdering, batchSize)
        .runWith(Sink.seq)
        .map(result => NewOrderingIds(currentMaxOrdering, result)) pipeTo self

    case NewOrderingIds(originalOffset, _) if originalOffset < currentMaxOrdering =>
      // search was done using an offset that became obsolete in the meantime
      // therefore we start a new query
      self ! QueryOrderingIds

    case NewOrderingIds(_, elements) =>
      findGaps(elements, currentMaxOrdering, missingByCounter, moduloCounter)

    case Status.Failure(t) =>
      val newDelay = maxBackoffQueryDelay.min(previousDelay * 2)
      if (newDelay == maxBackoffQueryDelay) {
        log.warning("Failed to query max ordering id because of {}, retrying in {}", t, newDelay)
      }
      scheduleQuery(newDelay)
      context.become(receive(currentMaxOrdering, missingByCounter, moduloCounter, newDelay))
  }

  /**
    * This method that implements the "find gaps" algo. It's the meat and main purpose of this actor.
    */
  def findGaps(
      elements: Seq[OrderingId],
      currentMaxOrdering: OrderingId,
      missingByCounter: Map[Int, MissingElements],
      moduloCounter: Int
  ): Unit = {

    // list of elements that will be considered as genuine gaps.
    // `givenUp` is either empty or is was filled on a previous iteration
    val givenUp = missingByCounter.getOrElse(moduloCounter, MissingElements.empty)

    val (nextMax, _, missingElems) =
      // using the ordering elements that were fetched, we verify if there are any gaps
      elements.foldLeft[(OrderingId, OrderingId, MissingElements)](
        currentMaxOrdering,
        currentMaxOrdering,
        MissingElements.empty
      ) {
        case ((currentMax, previousElement, missing), currentElement) =>
          // we must decide if we move the cursor forward
          val newMax =
            if ((currentMax + 1).untilWithForall(currentElement, index => givenUp.contains(index)))
              currentElement
            else currentMax

          // we accumulate in newMissing the gaps we detect on each iteration
          val newMissing =
            if (previousElement + 1 == currentElement || newMax == currentElement) missing
            else missing.addRange(previousElement + 1, currentElement)

          (newMax, currentElement, newMissing)
      }

    val newMissingByCounter = missingByCounter + (moduloCounter -> missingElems)

    // did we detect gaps in the current batch?
    val noGapsFound = missingElems.isEmpty

    // full batch means that we retrieved as much elements as the batchSize
    // that happens when we are not yet at the end of the stream
    val isFullBatch = elements.size == batchSize

    if (noGapsFound && isFullBatch) {
      // Many elements have been retrieved but none are missing
      // We can query again immediately, as this allows the actor to rapidly retrieve the real max ordering
      self ! QueryOrderingIds
      context.become(receive(nextMax, newMissingByCounter, moduloCounter))
    } else {
      // either we detected gaps or we reached the end of stream (batch not full)
      // in this case we want to keep querying but not immediately
      scheduleQuery(queryDelay)
      context.become(receive(nextMax, newMissingByCounter, (moduloCounter + 1) % maxTries))
    }
  }

  def scheduleQuery(delay: FiniteDuration): Unit = {
    timers.startSingleTimer(key = QueryOrderingIdsTimerKey, QueryOrderingIds, delay)
  }
}
