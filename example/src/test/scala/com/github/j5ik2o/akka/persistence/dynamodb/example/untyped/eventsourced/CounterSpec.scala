package com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced.CounterProtocol.{
  GetValueReply,
  SaveSnapshot
}
import com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced.CounterSpec.config
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBSpecSupport, RandomPortUtil }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.testcontainers.DockerClientFactory

import java.util.UUID
import scala.concurrent.duration.DurationInt
import com.typesafe.config.Config

object CounterSpec {
  val dynamoDBHost: String = DockerClientFactory.instance().dockerHostIpAddress()
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
  val legacyJournalMode    = false

  val config: Config = ConfigHelper
    .config(
      Some("persistence-reference"),
      legacyConfigFormat = false,
      legacyJournalMode = CounterSpec.legacyJournalMode,
      dynamoDBHost = CounterSpec.dynamoDBHost,
      dynamoDBPort = CounterSpec.dynamoDBPort,
      clientVersion = ClientVersion.V2.toString,
      clientType = ClientType.Async.toString
    )
}

class CounterSpec
    extends TestKit(ActorSystem("CounterSpec", CounterSpec.config))
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with ScalaFutures
    with DynamoDBSpecSupport {

  val testTimeFactor: Int = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt
  logger.debug(s"testTimeFactor = $testTimeFactor")

  implicit val pc: PatienceConfig = PatienceConfig(30.seconds, 1.seconds)

  override protected lazy val dynamoDBPort: Int = CounterSpec.dynamoDBPort

  override val legacyJournalTable: Boolean = CounterSpec.legacyJournalMode

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTable()
  }

  override def afterAll(): Unit = {
    deleteTable()
    super.afterAll()
  }

  def killActors(actors: ActorRef*): Unit = {
    actors.foreach { actorRef => system.stop(actorRef) }
  }

  "CounterSpec" - {
    "increment & getValue" in {
      val testProbe = TestProbe()
      val actorId   = UUID.randomUUID()

      {
        val counterRef = system.actorOf(Counter.props(actorId))
        counterRef ! CounterProtocol.Increment // seqNr = 1, 1,
        counterRef ! CounterProtocol.Increment // seqNr = 2, 2, saveSnapshot(2)
        counterRef ! CounterProtocol.Increment // seqNr = 3, 3,
        counterRef ! CounterProtocol.Increment // seqNr = 4, 4, saveSnapshot(4)

        counterRef ! CounterProtocol.GetValue(testProbe.ref)
        val reply = testProbe.expectMsgType[CounterProtocol.GetValueReply]((3 * testTimeFactor).seconds)
        assert(reply.n == 4)

        counterRef ! CounterProtocol.SaveSnapshot(testProbe.ref)
        testProbe.expectMsgType[CounterProtocol.SaveSnapshotReply]((3 * testTimeFactor).seconds)

        counterRef ! CounterProtocol.DeleteMessage(reply.seqNr, testProbe.ref)
        testProbe.expectMsgType[CounterProtocol.DeleteMessageReply]((3 * testTimeFactor).seconds)

        counterRef ! CounterProtocol.DeleteSnapshot(reply.seqNr, testProbe.ref)
        testProbe.expectMsgType[CounterProtocol.DeleteSnapshotReply]((3 * testTimeFactor).seconds)

        killActors(counterRef)
      }

      {
        val counterRef = system.actorOf(Counter.props(actorId))
        counterRef ! CounterProtocol.GetValue(testProbe.ref)
        val reply = testProbe.expectMsgType[GetValueReply]((3 * testTimeFactor).seconds)
        assert(reply.n == 0)
      }
    }
  }

}
