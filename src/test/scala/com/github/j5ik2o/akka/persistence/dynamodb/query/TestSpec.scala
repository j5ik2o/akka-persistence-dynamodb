package com.github.j5ik2o.akka.persistence.dynamodb.query

/*
 * Copyright 2017 Dennis Vriend
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

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import akka.event.{ Logging, LoggingAdapter }
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal

import scala.language.implicitConversions
//import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
//import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.inmemory.query.scaladsl.InMemoryReadJournal
//import akka.persistence.jdbc.config.JournalConfig
//import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal
//import akka.persistence.jdbc.util.SlickDatabase
import akka.persistence.journal.Tagged
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.scaladsl._
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ ActorMaterializer, Materializer }
import akka.testkit.TestProbe
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

abstract class TestSpec(config: String = "leveldb.conf")
    extends FlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Eventually {
  implicit val system: ActorSystem  = ActorSystem("test", ConfigFactory.load(config))
  implicit val mat: Materializer    = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  val log: LoggingAdapter           = Logging(system, this.getClass)
  implicit val pc: PatienceConfig   = PatienceConfig(timeout = 2.seconds)
  implicit val timeout: Timeout     = 30.seconds

  val identifier: String = config match {
    //    case "cassandra.conf" => CassandraReadJournal.Identifier
    case "inmemory.conf" => InMemoryReadJournal.Identifier
    //    case "jdbc.conf"      => JdbcReadJournal.Identifier
    case "leveldb.conf" => LeveldbReadJournal.Identifier
    case _              => DynamoDBReadJournal.Identifier
  }

  //  override def db: Option[JdbcBackend#DatabaseDef] = {
  //    Option(config).filter(_ == "jdbc.conf").map { _ =>
  //      val cfg = system.settings.config.getConfig("jdbc-journal")
  //      val journalConfig = new JournalConfig(cfg)
  //      SlickDatabase.forConfig(cfg, journalConfig.slickConfiguration)
  //    }
  //  }

  //  def session: Option[CassandraSession] = {
  //    Option(config).filter(_ == "cassandra.conf").map { _ =>
  //      readJournal.asInstanceOf[CassandraReadJournal].session
  //    }
  //  }

  val readJournal: ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery = {
    PersistenceQuery(system).readJournalFor(identifier)
  }

  def randomId = UUID.randomUUID.toString.take(5)

  def terminate(actors: ActorRef*): Unit = {
    val tp = TestProbe()
    actors.foreach { (actor: ActorRef) =>
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
    system.actorOf(Props(new TestActor(persistenceId)))
  }

  def deleteEvents(actor: ActorRef, toSequenceNr: Long): Future[Unit] = {
    import akka.pattern.ask
    actor.ask(TestActor.DeleteCmd(toSequenceNr)).map(_ => ())
  }

  def clearEventStore(actors: ActorRef*): Future[Unit] = {
    import akka.pattern.ask
    Future.sequence(actors.map(_.ask(TestActor.DeleteCmd()))).map(_ => ())
  }

  def withTestActors()(f: (ActorRef, ActorRef, ActorRef) => Unit): Unit = {
    val actor1 = setupEmpty(1)
    val actor2 = setupEmpty(2)
    val actor3 = setupEmpty(3)
    try f(actor1, actor2, actor3)
    finally {
      //      clearEventStore(actor1, actor2, actor3).toTry should be a 'success
      terminate(actor1, actor2, actor3)
    }
  }

  def sendMessage(msg: Any, actors: ActorRef*): Future[Unit] = {
    import akka.pattern.ask
    Future.sequence(actors.map(_.ask(msg))).map(_ => ())
  }

  def withTags(msg: Any, tags: String*) = Tagged(msg, Set(tags: _*))

  def sendMessage(tagged: Tagged, actors: ActorRef*): Future[Unit] = {
    import akka.pattern.ask
    Future.sequence(actors.map(_.ask(tagged))).map(_ => ())
  }

  val WITH_IN = 30 seconds

  def withCurrentPersistenceIds(within: FiniteDuration = WITH_IN)(f: TestSubscriber.Probe[String] => Unit): Unit = {
    val tp = readJournal
      .currentPersistenceIds().filter { pid =>
        log.info(s"withCurrentPersistenceIds:filter = $pid")
        (1 to 3).map(id => s"my-$id").contains(pid)
      }.runWith(
        TestSink.probe[String]
      )
    tp.within(within)(f(tp))
  }

  def withPersistenceIds(within: FiniteDuration = WITH_IN)(f: TestSubscriber.Probe[String] => Unit): Unit = {
    val tp = readJournal
      .persistenceIds().filter { pid =>
        log.info(s"withPersistenceIds:filter = $pid")
        (1 to 3).map(id => s"my-$id").contains(pid)
      }.runWith(TestSink.probe[String])
    tp.within(within)(f(tp))
  }

  /**
    * The returned event stream is ordered by sequence number,
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
    config match {
      //      case "cassandra.conf" =>
      //        println("Cleaning Cassandra Journal")
      //        session.foreach { s =>
      //          Future.sequence(List(
      //            s.executeWrite("TRUNCATE akka.messages")
      //                      s.executeWrite("TRUNCATE akka.metadata"),
      //                      s.executeWrite("TRUNCATE akka.config"),
      //                      s.executeWrite("TRUNCATE akka.snapshots")
      //          )).futureValue
      //        }
      //      case "jdbc.conf" =>
      //        println("Closing database connections")
      //        db.foreach(_.close())
      case "inmemory.conf" =>
        println("No cleanup for inmemory store")
      case "leveldb.conf" =>
        println("Deleting LevelDb dirs: " + deleteDirs)
      case _ =>
    }
    TestKit.shutdownActorSystem(system)
  }

  override protected def beforeEach(): Unit = {}

  override protected def beforeAll(): Unit = {
    //    config match {
    //      case "jdbc.conf" =>
    //        dropCreate(H2())
    //      case _ =>
    //    }
  }

  def countJournal: Long = {
    log.info("countJournal: start")
    val numEvents = readJournal
      .currentPersistenceIds()
      .filter { pid =>
        log.info(s"filter: pid = $pid")
        (1 to 3).map(id => s"my-$id").contains(pid)
      }
      .mapAsync(1) { pid =>
        log.info(s"mapAsync:pid = $pid")
        val result = readJournal.currentEventsByPersistenceId(pid, 0, Long.MaxValue).map(_ => 1).runFold(0)(_ + _)
        log.info(s"result = $result")
        result
      }.runFold(0)(_ + _)
      .futureValue
    log.info("==> NumEvents: " + numEvents)
    log.info("countJournal: ==> NumEvents: " + numEvents)
    numEvents
  }

  def deleteDirs: (Boolean, Boolean) = {
    def loop(dir: java.io.File): Unit = {
      Option(dir.listFiles).foreach(_.filter(_.isFile).foreach { file =>
        println(s"Deleting: ${file.getName}: ${file.delete}")
      })
    }

    val journalDir   = new java.io.File("target/journal")
    val snapshotsDir = new java.io.File("target/snapshots")
    loop(journalDir)
    loop(snapshotsDir)
    (journalDir.delete(), snapshotsDir.delete())
  }

  implicit class FutureSequence[A](xs: Seq[Future[A]]) {
    def sequence(implicit ec: ExecutionContext): Future[Seq[A]] = Future.sequence(xs)
    def toTry(implicit ec: ExecutionContext): Try[Seq[A]]       = Future.sequence(xs).toTry
  }
}

//import java.sql.Statement
//
//import akka.actor.ActorSystem
//import slick.jdbc.JdbcBackend

//object Schema {
//
//  sealed trait SchemaType { def schema: String }
//  final case class H2(schema: String = "schema/h2/h2-schema.sql") extends SchemaType
//}

//trait DropCreate extends ClasspathResources {
//
//  def system: ActorSystem
//
//  def db: Option[JdbcBackend#Database]
//
//  def dropCreate(schemaType: SchemaType): Unit = schemaType match {
//    case s: SchemaType => create(s.schema)
//  }
//
//  def create(schema: String, separator: String = ";"): Unit = for {
//    schema <- Option(fromClasspathAsString(schema))
//    ddl <- for {
//      trimmedLine <- schema.split(separator) map (_.trim)
//      if trimmedLine.nonEmpty
//    } yield trimmedLine
//  } withStatement { stmt =>
//    try stmt.executeUpdate(ddl) catch {
//      case t: java.sql.SQLSyntaxErrorException if t.getMessage contains "ORA-00942" => // suppress known error message in the test
//    }
//  }
//
//  def withDatabase[A](f: JdbcBackend#Database => A): Option[A] =
//    db.map(f(_))
//
//  def withSession[A](f: JdbcBackend#Session => A): Option[A] = {
//    withDatabase { db =>
//      val session = db.createSession()
//      try f(session) finally session.close()
//    }
//  }
//
//  def withStatement[A](f: Statement => A): Option[A] =
//    withSession(session => session.withStatement()(f))
//}
//
//import java.io.InputStream
//
//import akka.NotUsed
//import akka.stream.IOResult
//import akka.stream.scaladsl.{ Source, StreamConverters }
//import akka.util.ByteString
//
//import scala.concurrent.Future
//import scala.io.{ Source => ScalaIOSource }
//import scala.util.Try
//import scala.xml.pull.{ XMLEvent, XMLEventReader }
//
//object ClasspathResources extends ClasspathResources
//
//trait ClasspathResources {
//  def withInputStream[T](fileName: String)(f: InputStream => T): T = {
//    val is = fromClasspathAsStream(fileName)
//    try {
//      f(is)
//    } finally {
//      Try(is.close())
//    }
//  }
//
//  def withXMLEventReader[T](fileName: String)(f: XMLEventReader => T): T =
//    withInputStream(fileName) { is =>
//      f(new XMLEventReader(ScalaIOSource.fromInputStream(is)))
//    }
//
//  def withXMLEventSource[T](fileName: String)(f: Source[XMLEvent, NotUsed] => T): T =
//    withXMLEventReader(fileName) { reader =>
//      f(Source.fromIterator(() => reader))
//    }
//
//  def withByteStringSource[T](fileName: String)(f: Source[ByteString, Future[IOResult]] => T): T =
//    withInputStream(fileName) { inputStream =>
//      f(StreamConverters.fromInputStream(() => inputStream))
//    }
//
//  def streamToString(is: InputStream): String =
//    ScalaIOSource.fromInputStream(is).mkString
//
//  def fromClasspathAsString(fileName: String): String =
//    streamToString(fromClasspathAsStream(fileName))
//
//  def fromClasspathAsStream(fileName: String): InputStream =
//    getClass.getClassLoader.getResourceAsStream(fileName)
//
//}
