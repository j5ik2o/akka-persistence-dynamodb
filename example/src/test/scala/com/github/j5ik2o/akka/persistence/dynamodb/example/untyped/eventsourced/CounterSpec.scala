package com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, QueryRequest }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced.CounterProtocol.GetValueReply
import com.github.j5ik2o.akka.persistence.dynamodb.journal.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBSpecSupport, RandomPortUtil }
import com.typesafe.config.Config
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.testcontainers.DockerClientFactory

import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object CounterSpec {
  val dynamoDBHost: String = DockerClientFactory.instance().dockerHostIpAddress()
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
  val legacyJournalMode    = false
  val softDelete           = true

  val config: Config = ConfigHelper
    .config(
      Some("persistence-reference"),
      legacyConfigFormat = false,
      legacyJournalMode = CounterSpec.legacyJournalMode,
      dynamoDBHost = CounterSpec.dynamoDBHost,
      dynamoDBPort = CounterSpec.dynamoDBPort,
      clientVersion = ClientVersion.V1.toString,
      clientType = ClientType.Async.toString,
      softDelete = softDelete
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
        counterRef ! CounterProtocol.Increment // seqNr = 2, 1, saveSnapshot(2)
        counterRef ! CounterProtocol.Increment // seqNr = 3, 1,
        counterRef ! CounterProtocol.Increment // seqNr = 4, 1, saveSnapshot(4)

        counterRef ! CounterProtocol.GetValue(testProbe.ref)
        val reply = testProbe.expectMsgType[CounterProtocol.GetValueReply]((10 * testTimeFactor).seconds)
        assert(reply.n == 4)

        counterRef ! CounterProtocol.SaveSnapshot(testProbe.ref)
        testProbe.expectMsgType[CounterProtocol.SaveSnapshotReply]((10 * testTimeFactor).seconds)

        counterRef ! CounterProtocol.DeleteMessage(reply.seqNr - 1, testProbe.ref)
        testProbe.expectMsgType[CounterProtocol.DeleteMessageReply]((10 * testTimeFactor).seconds)

        counterRef ! CounterProtocol.DeleteSnapshot(reply.seqNr, testProbe.ref)
        testProbe.expectMsgType[CounterProtocol.DeleteSnapshotReply]((10 * testTimeFactor).seconds)

        killActors(counterRef)
      }

      {
        val counterRef = system.actorOf(Counter.props(actorId))
        counterRef ! CounterProtocol.GetValue(testProbe.ref)
        val reply = testProbe.expectMsgType[GetValueReply]((3 * testTimeFactor).seconds)
        assert(reply.n == 1)
      }

      val pluginConfig = JournalPluginConfig.fromConfig(CounterSpec.config.getConfig("j5ik2o.dynamo-db-journal"))

      val persistenceId  = Counter.pid(actorId)
      val fromSequenceNr = 0
      val toSequenceNr   = Long.MaxValue
      val deleted        = None
      val limit          = Int.MaxValue
      val queryRequest = new QueryRequest()
        .withTableName(journalTableName)
        .withIndexName(
          GetJournalRowsIndexName
        ).withKeyConditionExpression(
          "#pid = :pid and #snr between :min and :max"
        ).withFilterExpression(deleted.map { _ => s"#flg = :flg" }.orNull)
        .withExpressionAttributeNames(
          (Map(
            "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName,
            "#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName
          ) ++ deleted
            .map(_ => Map("#flg" -> pluginConfig.columnsDefConfig.deletedColumnName)).getOrElse(Map.empty)).asJava
        )
        .withExpressionAttributeValues(
          (Map(
            ":pid" -> new AttributeValue().withS(persistenceId),
            ":min" -> new AttributeValue().withN(fromSequenceNr.toString),
            ":max" -> new AttributeValue().withN(toSequenceNr.toString)
          ) ++ deleted.map(b => Map(":flg" -> new AttributeValue().withBOOL(b))).getOrElse(Map.empty)).asJava
        ).withLimit(limit)
      val result = dynamoDBClient.query(queryRequest)

      if (result.getCount > 0) {
        val items = result.getItems.asScala.map(_.asScala).toVector
        items.foreach { item =>
          println(
            Seq(
              "pkey"           -> item("pkey").toString,
              "skey"           -> item("skey").toString,
              "persistence-id" -> item("persistence-id").toString,
              "sequence-nr"    -> item("sequence-nr").toString,
              "deleted"        -> item("deleted").toString
            ).map { case (k, v) => s"$k = $v" }.mkString("[\n", ", \n", "\n]")
          )
        }

        if (!CounterSpec.softDelete) {
          assert(items.size == 1)
          assert(items(0)("persistence-id").getS == Counter.pid(actorId))
          assert(items(0)("sequence-nr").getN.toInt == 4)
          assert(items(0)("deleted").getBOOL == false)
        } else {
          assert(items.size == 4)
          items.zipWithIndex.foreach { case (item, idx) =>
            assert(item("persistence-id").getS == Counter.pid(actorId))
            assert(item("sequence-nr").getN.toInt == idx + 1)
            idx + 1 match {
              case n if 1 <= n && n <= 3 =>
                assert(item("deleted").getBOOL == true)
              case 4 =>
                assert(item("deleted").getBOOL == false)
            }
          }
        }
      }
    }
  }

}
