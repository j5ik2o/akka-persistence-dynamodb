package akka.persistence.journal

import java.net.URI

import akka.actor.{ Actor, ActorRef, ActorSystem }
import akka.persistence.JournalProtocol._
import akka.persistence._
import akka.persistence.scalatest.{ MayVerb, OptionalTests }
import akka.testkit.TestProbe
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.ConfigHelper
import com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka.dao.{
  KafkaJournalRowWriteDriver,
  KafkaToDynamoDBProjector
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import net.manub.embeddedkafka.{ EmbeddedK, EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.concurrent.ScalaFutures
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.collection.immutable.Seq
import scala.concurrent.duration._

object DynamoDBJournalKafkaWritingSpec {
  val dynamoDBPort: Int          = RandomPortUtil.temporaryServerPort()
  val legacyJournalMode: Boolean = false
  val kafkaPort                  = RandomPortUtil.temporaryServerPort()
  val zkPort                     = RandomPortUtil.temporaryServerPort()
}

class DynamoDBJournalKafkaWritingSpec
    extends PluginSpec(
      ConfigHelper.config(
        legacyConfigFormat = false,
        legacyJournalMode = DynamoDBJournalKafkaWritingSpec.legacyJournalMode,
        dynamoDBPort = DynamoDBJournalKafkaWritingSpec.dynamoDBPort,
        clientVersion = ClientVersion.V2,
        clientType = ClientType.Async,
        journalRowDriverWrapperClassName = Some(classOf[KafkaJournalRowWriteDriver].getName),
        kafkaPort = Some(DynamoDBJournalKafkaWritingSpec.kafkaPort)
      )
    )
    with MayVerb
    with OptionalTests
    with JournalCapabilityFlags
    with ScalaFutures
    with DynamoDBSpecSupport {
  implicit lazy val system: ActorSystem = ActorSystem("JournalSpec", config.withFallback(JournalSpec.config))

  implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  private var senderProbe: TestProbe   = _
  private var receiverProbe: TestProbe = _

  override protected def supportsSerialization: CapabilityFlag = true
  var kafka: EmbeddedK                                         = _

  def setUp: Unit = synchronized {
    deleteTable()
    createTable()
    implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      kafkaPort = DynamoDBJournalKafkaWritingSpec.kafkaPort,
      zooKeeperPort = DynamoDBJournalKafkaWritingSpec.zkPort,
      customBrokerProperties = Map(
        "num.partitions" -> "8"
      )
    )
    if (kafka != null)
      kafka.stop(true)
    kafka = EmbeddedKafka.start()
    projector.start("p")
  }

  def tearDown: Unit = synchronized {
    projector.stop()
    kafka.stop(true)
    kafka = null
    deleteTable()
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    setUp
  }

  protected override def afterAll(): Unit = {
    tearDown
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    senderProbe = TestProbe()
    receiverProbe = TestProbe()
    preparePersistenceId(pid)
    writeMessages(1, 5, pid, senderProbe.ref, writerUuid)
    Thread.sleep(1000 * 5)
  }

  /**
    * Overridable hook that is called before populating the journal for the next
    * test case. `pid` is the `persistenceId` that will be used in the test.
    * This method may be needed to clean pre-existing events from the log.
    */
  def preparePersistenceId(pid: String): Unit = ()

  /**
    * Implementation may override and return false if it does not
    * support atomic writes of several events, as emitted by `persistAll`.
    */
  def supportsAtomicPersistAllOfSeveralEvents: Boolean = true

  def journal: ActorRef =
    extension.journalFor(null)

  def replayedMessage(snr: Long, deleted: Boolean = false, confirms: Seq[String] = Nil): ReplayedMessage =
    ReplayedMessage(PersistentImpl(s"a-${snr}", snr, pid, "", deleted, Actor.noSender, writerUuid, 0L))

  def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    def persistentRepr(sequenceNr: Long) =
      PersistentRepr(
        payload = s"a-$sequenceNr",
        sequenceNr = sequenceNr,
        persistenceId = pid,
        sender = sender,
        writerUuid = writerUuid
      )

    val msgs =
      if (supportsAtomicPersistAllOfSeveralEvents) {
        (fromSnr to toSnr - 1).map { i =>
          if (i == toSnr - 1)
            AtomicWrite(List(persistentRepr(i), persistentRepr(i + 1)))
          else
            AtomicWrite(persistentRepr(i))
        }
      } else {
        (fromSnr to toSnr).map { i => AtomicWrite(persistentRepr(i)) }
      }

    val probe = TestProbe()

    journal ! WriteMessages(msgs, probe.ref, actorInstanceId)

    probe.expectMsg(WriteMessagesSuccessful)
    (fromSnr to toSnr).foreach { i =>
      probe.expectMsgPF() {
        case WriteMessageSuccess(PersistentImpl(payload, `i`, `pid`, _, _, `sender`, `writerUuid`, _), _) =>
          payload should be(s"a-${i}")
      }
    }
  }

  val journalPluginConfig = JournalPluginConfig.fromConfig(system.settings.config.getConfig("j5ik2o.dynamo-db-journal"))

  val projector: KafkaToDynamoDBProjector = new KafkaToDynamoDBProjector(system, journalPluginConfig)

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override protected lazy val dynamoDBPort: Int = DynamoDBJournalKafkaWritingSpec.dynamoDBPort

  override val legacyJournalTable: Boolean = DynamoDBJournalKafkaWritingSpec.legacyJournalMode

  val underlying: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def dynamoDbAsyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(underlying)

  val timeout = 30 seconds

  "A journal" must {
    "replay all messages" in {
      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      (1 to 5).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
      Thread.sleep(1000)
      receiverProbe.expectMsg(RecoverySuccess(highestSequenceNr = 5L))
    }
    "replay messages using a lower sequence number bound" in {
      journal ! ReplayMessages(3, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      (3 to 5).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
      receiverProbe.expectMsg(RecoverySuccess(highestSequenceNr = 5L))
    }
    "replay messages using an upper sequence number bound" in {
      journal ! ReplayMessages(1, 3, Long.MaxValue, pid, receiverProbe.ref)
      (1 to 3).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
    }
    "replay messages using a count limit" in {
      journal ! ReplayMessages(1, Long.MaxValue, 3, pid, receiverProbe.ref)
      (1 to 3).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
    }
    "replay messages using a lower and upper sequence number bound" in {
      journal ! ReplayMessages(2, 3, Long.MaxValue, pid, receiverProbe.ref)
      (2 to 3).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
    }
    "replay messages using a lower and upper sequence number bound and a count limit" in {
      journal ! ReplayMessages(2, 5, 2, pid, receiverProbe.ref)
      (2 to 3).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
    }
    "replay a single if lower sequence number bound equals upper sequence number bound" in {
      journal ! ReplayMessages(2, 2, Long.MaxValue, pid, receiverProbe.ref)
      (2 to 2).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
    }
    "replay a single message if count limit equals 1" in {
      journal ! ReplayMessages(2, 4, 1, pid, receiverProbe.ref)
      (2 to 2).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
    }
    "not replay messages if count limit equals 0" in {
      journal ! ReplayMessages(2, 4, 0, pid, receiverProbe.ref)
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
    }
    "not replay messages if lower  sequence number bound is greater than upper sequence number bound" in {
      journal ! ReplayMessages(3, 2, Long.MaxValue, pid, receiverProbe.ref)
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
    }
    "not replay messages if the persistent actor has not yet written messages" in {
      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, "non-existing-pid", receiverProbe.ref)
      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 0L))
    }
//    "not replay permanently deleted messages (range deletion)" in {
//      val receiverProbe2 = TestProbe()
//      val cmd            = DeleteMessagesTo(pid, 3, receiverProbe2.ref)
//      val sub            = TestProbe()
//
//      subscribe[DeleteMessagesTo](sub.ref)
//      journal ! cmd
//      sub.expectMsg(timeout, cmd)
//      receiverProbe2.expectMsg(timeout, DeleteMessagesSuccess(cmd.toSequenceNr))
//
//      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
//      List(4, 5).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
//
//      receiverProbe2.expectNoMsg(200.millis)
//    }

//    "not reset highestSequenceNr after message deletion" in {
//      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
//      (1 to 5).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
//      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
//
//      journal ! DeleteMessagesTo(pid, 3L, receiverProbe.ref)
//      receiverProbe.expectMsg(timeout, DeleteMessagesSuccess(3L))
//
//      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
//      (4 to 5).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
//      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
//    }
//
//    "not reset highestSequenceNr after journal cleanup" in {
//      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
//      (1 to 5).foreach { i => receiverProbe.expectMsg(timeout, replayedMessage(i)) }
//      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
//
//      journal ! DeleteMessagesTo(pid, Long.MaxValue, receiverProbe.ref)
//      receiverProbe.expectMsg(timeout, DeleteMessagesSuccess(Long.MaxValue))
//
//      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
//      receiverProbe.expectMsg(timeout, RecoverySuccess(highestSequenceNr = 5L))
//    }
  }
}
