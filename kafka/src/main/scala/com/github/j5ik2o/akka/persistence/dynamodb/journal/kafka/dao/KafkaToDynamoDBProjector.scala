package com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka.dao

import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.kafka.{ CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions }
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Unzip, Zip }
import akka.stream.{ Attributes, FlowShape }
import akka.{ Done, NotUsed }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.{
  createV1DaxJournalRowWriteDriver,
  createV1JournalRowWriteDriver,
  createV2JournalRowWriteDriver
}
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka.dao.KafkaToDynamoDBProjector.{
  Start,
  Started,
  Stop,
  Stopped
}
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext }

object KafkaToDynamoDBProjector {

  sealed trait Message
  case class Start(topics: String*) extends Message
  case class Started()              extends Message
  case class Stop()                 extends Message
  case class Stopped()              extends Message

}

class KafkaToDynamoDBProjector(system: ActorSystem, journalPluginConfig: JournalPluginConfig) {
  implicit val s = system
  import system.dispatcher
  implicit val _log = system.log

  private val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess

  private val config = journalPluginConfig.sourceConfig.getConfig("kafka-to-ddb-projector")

  private val parallelism: Int        = config.getInt("parallelism")
  private val consumerConfig: Config  = config.getConfig("consumer")
  private val committerConfig: Config = config.getConfig("committer-settings")

  protected val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)

  private val committerSettings: CommitterSettings = CommitterSettings(committerConfig)
  private var control: DrainingControl[Done]       = _

  private var javaAsyncClientV2: JavaDynamoDbAsyncClient = _
  private var javaSyncClientV2: JavaDynamoDbSyncClient   = _

  private val partitionKeyResolver: PartitionKeyResolver = {
    val provider = PartitionKeyResolverProvider.create(dynamicAccess, journalPluginConfig)
    provider.create
  }

  private val sortKeyResolver: SortKeyResolver = {
    val provider = SortKeyResolverProvider.create(dynamicAccess, journalPluginConfig)
    provider.create
  }

  protected val metricsReporter: Option[MetricsReporter] = {
    val metricsReporterProvider = MetricsReporterProvider.create(dynamicAccess, journalPluginConfig)
    metricsReporterProvider.create
  }

  private val journalRowWriteDriver: JournalRowWriteDriver = {
    journalPluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V2 =>
        createV2JournalRowWriteDriver(
          system,
          dynamicAccess,
          journalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )(v1 => javaSyncClientV2 = v1, v2 => javaAsyncClientV2 = v2)
      case ClientVersion.V1 =>
        createV1JournalRowWriteDriver(
          system,
          dynamicAccess,
          journalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )
      case ClientVersion.V1Dax =>
        createV1DaxJournalRowWriteDriver(
          system,
          journalPluginConfig,
          partitionKeyResolver,
          sortKeyResolver,
          metricsReporter
        )
    }
  }

  private def projectionFlow(
      implicit ec: ExecutionContext
  ): Flow[ConsumerMessage.CommittableMessage[String, Array[Byte]], CommittableOffset, NotUsed] = {
    Flow[ConsumerMessage.CommittableMessage[String, Array[Byte]]]
      .map { message: ConsumerMessage.CommittableMessage[String, Array[Byte]] =>
        val sequenceNumber = new String(
          message.record.headers.lastHeader(KafkaJournalRowWriteDriver.SequenceNumberHeaderKey).value()
        ).toLong
        val deleted =
          new String(message.record.headers().lastHeader(KafkaJournalRowWriteDriver.DeletedHeaderKey).value()).toBoolean
        val ordering =
          new String(message.record.headers().lastHeader(KafkaJournalRowWriteDriver.OrderingHeaderKey).value()).toLong
        (
          JournalRow(
            persistenceId = PersistenceId(message.record.key()),
            sequenceNumber = SequenceNumber(sequenceNumber),
            deleted,
            message = message.record.value(),
            ordering,
            tags = None
          ),
          message.committableOffset
        )
      }.via(Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val unzip = b.add(Unzip[JournalRow, CommittableOffset]())
        val zip   = b.add(Zip[Long, CommittableOffset]())
        unzip.out0 ~> journalRowWriteDriver.singlePutJournalRowFlow ~> zip.in0
        unzip.out1 ~> zip.in1
        FlowShape(unzip.in, zip.out)
      })).map(_._2).log("projection-flow")
  }

  def stop(): Unit = {
    require(control != null)
    Await.result(control.drainAndShutdown(), Duration.Inf)
    control = null
  }

  def start(topics: String*): Unit = {
    require(control == null)
    control = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics(topics: _*))
      .log("kafka source")
      .flatMapMerge(parallelism, _._2)
      .via(projectionFlow)
      .via(Committer.flow(committerSettings))
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl(_))
      .addAttributes(
        Attributes.logLevels(
          onElement = Attributes.LogLevels.Info,
          onFinish = Attributes.LogLevels.Info,
          onFailure = Attributes.LogLevels.Error
        )
      )
      .run()
  }

//  override def receive: Receive = {
//    case Start(topics) =>
//      start(topics)
//      sender() ! Started()
//    case Stop() =>
//      stop()
//      sender() ! Stopped()
//  }

}
