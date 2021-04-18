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

import java.util.UUID
import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import akka.event.{ Logging, LoggingAdapter }
import akka.persistence.journal.Tagged
import akka.persistence.query.scaladsl._
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import com.typesafe.config.Config
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

abstract class QueryJournalSpec(config: Config)
    extends AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Eventually {
  implicit val system: ActorSystem  = ActorSystem("test", config)
  implicit val ec: ExecutionContext = system.dispatcher
  val log: LoggingAdapter           = Logging(system, this.getClass)
  implicit val pc: PatienceConfig   = PatienceConfig(timeout = 2.seconds)
  implicit val timeout: Timeout     = 30.seconds

  implicit val mat: ActorMaterializer = ActorMaterializer()

  val identifier: String = DynamoDBReadJournal.Identifier

  val readJournal: ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery = {
    PersistenceQuery(system).readJournalFor(identifier)
  }

  def randomId: String = UUID.randomUUID.toString.take(5)

  def terminate(actors: ActorRef*): Unit = {
    val tp = TestProbe()
    actors.foreach { actor: ActorRef =>
      tp watch actor
      actor ! PoisonPill
      tp expectTerminated actor
    }
  }

  implicit def numericValueToOffsetValue(value: Int): Offset = Offset.sequence(value)

  implicit class PimpedFuture[T](self: Future[T]) {
    def toTry: Try[T] = Try(self.futureValue)
  }

  def setupEmpty(persistenceId: Int): ActorRef = {
    system.actorOf(Props(new PersistenceTestActor(persistenceId)))
  }

  def deleteEvents(actor: ActorRef, toSequenceNr: Long): Future[Unit] = {
    import akka.pattern.ask
    actor.ask(PersistenceTestActor.DeleteCmd(toSequenceNr)).map(_ => ())
  }

  def clearEventStore(actors: ActorRef*): Future[Unit] = {
    import akka.pattern.ask
    Future.sequence(actors.map(_.ask(PersistenceTestActor.DeleteCmd()))).map(_ => ())
  }

  def withTestActors()(f: (ActorRef, ActorRef, ActorRef) => Unit): Unit = {
    val actor1 = setupEmpty(1)
    val actor2 = setupEmpty(2)
    val actor3 = setupEmpty(3)
    try f(actor1, actor2, actor3)
    finally {
      //      clearEventStore(actor1, actor2, actor3).toTry should be a Symbol("success")
      terminate(actor1, actor2, actor3)
    }
  }

  def sendMessage(msg: Any, actors: ActorRef*): Future[Unit] = {
    import akka.pattern.ask
    Future.sequence(actors.map(_.ask(msg))).map(_ => ())
  }

  def withTags(msg: Any, tags: String*): Tagged = Tagged(msg, Set(tags: _*))

  def sendMessage(tagged: Tagged, actors: ActorRef*): Future[Unit] = {
    import akka.pattern.ask
    Future.sequence(actors.map(_.ask(tagged))).map(_ => ())
  }

  val WITH_IN: FiniteDuration = 30 seconds

  def withCurrentPersistenceIds(within: FiniteDuration = WITH_IN)(f: TestSubscriber.Probe[String] => Unit): Unit = {
    val tp = readJournal
      .currentPersistenceIds().filter { pid =>
        log.debug(s"withCurrentPersistenceIds:filter = $pid")
        (1 to 3).map(id => s"my-$id").contains(pid)
      }.runWith(
        TestSink.probe[String]
      )
    tp.within(within)(f(tp))
  }

  def withPersistenceIds(within: FiniteDuration = WITH_IN)(f: TestSubscriber.Probe[String] => Unit): Unit = {
    val tp = readJournal
      .persistenceIds().filter { pid =>
        log.debug(s"withPersistenceIds:filter = $pid")
        (1 to 3).map(id => s"my-$id").contains(pid)
      }.runWith(TestSink.probe[String])
    tp.within(within)(f(tp))
  }

  /** The returned event stream is ordered by sequence number,
    * i.e. the same order as the PersistentActor persisted the events.
    * The same prefix of stream elements (in same order) are returned for multiple
    * executions of the query, except for when events have been deleted.
    */
  def withCurrentEventsByPersistenceId(within: FiniteDuration = WITH_IN)(
      persistenceId: String,
      fromSequenceNr: Long = 0,
      toSequenceNr: Long = Long.MaxValue
  )(f: TestSubscriber.Probe[EventEnvelope] => Unit): Unit = {
    val tp = readJournal
      .currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).runWith(TestSink.probe[EventEnvelope])
    tp.within(within)(f(tp))
  }

  def withEventsByPersistenceId(within: FiniteDuration = WITH_IN)(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  )(f: TestSubscriber.Probe[EventEnvelope] => Unit): Unit = {
    val tp = readJournal
      .eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).runWith(TestSink.probe[EventEnvelope])
    tp.within(within)(f(tp))
  }

  def withCurrentEventsByTag(
      within: FiniteDuration = WITH_IN
  )(tag: String, offset: Offset)(f: TestSubscriber.Probe[EventEnvelope] => Unit): Unit = {
    val tp = readJournal.currentEventsByTag(tag, offset).runWith(TestSink.probe[EventEnvelope])
    tp.within(within)(f(tp))
  }

  def withEventsByTag(
      within: FiniteDuration = WITH_IN
  )(tag: String, offset: Offset)(f: TestSubscriber.Probe[EventEnvelope] => Unit): Unit = {
    val tp = readJournal.eventsByTag(tag, offset).runWith(TestSink.probe[EventEnvelope])
    tp.within(within)(f(tp))
  }

  def currentEventsByTagAsList(tag: String, offset: Offset): List[EventEnvelope] =
    readJournal.currentEventsByTag(tag, offset).runWith(Sink.seq).futureValue.toList

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  override protected def beforeEach(): Unit = {}

  override protected def beforeAll(): Unit = {}

  def countJournal: Long = {
    log.debug("countJournal: start")
    val numEvents = readJournal
      .currentPersistenceIds()
      .filter { pid =>
        log.debug(s"filter: pid = $pid")
        (1 to 3).map(id => s"my-$id").contains(pid)
      }
      .mapAsync(1) { pid =>
        log.debug(s"mapAsync:pid = $pid")
        val result = readJournal.currentEventsByPersistenceId(pid, 0, Long.MaxValue).map(_ => 1).runFold(0)(_ + _)
        log.debug(s"result = $result")
        result
      }.runFold(0)(_ + _)
      .futureValue
    log.debug("==> NumEvents: " + numEvents)
    log.debug("countJournal: ==> NumEvents: " + numEvents)
    numEvents
  }

  implicit class FutureSequence[A](xs: Seq[Future[A]]) {
    def sequence(implicit ec: ExecutionContext): Future[Seq[A]] = Future.sequence(xs)
    def toTry(implicit ec: ExecutionContext): Try[Seq[A]]       = Future.sequence(xs).toTry
  }

}
