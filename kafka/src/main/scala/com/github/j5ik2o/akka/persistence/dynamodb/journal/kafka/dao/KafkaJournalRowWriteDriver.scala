package com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka.dao

import java.nio.charset.StandardCharsets
import java.util

import akka.NotUsed
import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.kafka.scaladsl.Producer
import akka.kafka.{ ProducerMessage, ProducerSettings }
import akka.stream.scaladsl.{ Flow, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ JournalRowWriteDriver, PersistenceIdWithSeqNr }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka.{
  KafkaPartitionKeyResolver,
  KafkaPartitionKeyResolverProvider,
  KafkaPartitionSize,
  KafkaPartitionSizeResolver,
  KafkaPartitionSizeResolverProvider,
  KafkaTopicResolver,
  KafkaTopicResolverProvider,
  KafkaWriteAdaptorConfig,
  PartitionKeyResolverType
}
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{
  JournalRow,
  PersistenceId,
  SequenceNumber,
  ToPersistenceIdOps
}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

import scala.jdk.CollectionConverters._

object KafkaJournalRowWriteDriver {
  final val PersistenceIdHeaderKey  = "persistenceId"
  final val SequenceNumberHeaderKey = "sequenceNumber"
  final val DeletedHeaderKey        = "deleted"
  final val OrderingHeaderKey       = "ordering"
}

class KafkaJournalRowWriteDriver(
    journalPluginConfig: JournalPluginConfig,
    underlying: JournalRowWriteDriver
) extends JournalRowWriteDriver {
  import KafkaJournalRowWriteDriver._

  def system: ActorSystem = underlying.system

  private val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess

  private val journalPluginSourceConfig = journalPluginConfig.sourceConfig

  private val kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig =
    KafkaWriteAdaptorConfig.fromConfig(journalPluginSourceConfig.getConfig("kafka-write-adaptor"))

  private val kafkaTopicResolverProvider: KafkaTopicResolverProvider =
    KafkaTopicResolverProvider.create(dynamicAccess, kafkaWriteAdaptorConfig)
  private val kafkaTopicResolver: KafkaTopicResolver = kafkaTopicResolverProvider.create

  private val maybeKafkaPartitionKeyResolver: Option[KafkaPartitionKeyResolver] =
    kafkaWriteAdaptorConfig.partitionKeyResolverType match {
      case PartitionKeyResolverType.Auto => None
      case PartitionKeyResolverType.Manual =>
        val kafkaPartitionKeyResolverProvider: KafkaPartitionKeyResolverProvider =
          KafkaPartitionKeyResolverProvider.create(dynamicAccess, kafkaWriteAdaptorConfig)
        Some(kafkaPartitionKeyResolverProvider.create)
    }

  private val maybeKafkaPartitionSizeResolver: Option[KafkaPartitionSizeResolver] =
    kafkaWriteAdaptorConfig.partitionKeyResolverType match {
      case PartitionKeyResolverType.Auto => None
      case PartitionKeyResolverType.Manual =>
        val kafkaPartitionSizeResolverProvider: KafkaPartitionSizeResolverProvider =
          KafkaPartitionSizeResolverProvider.create(dynamicAccess, kafkaWriteAdaptorConfig)
        Some(kafkaPartitionSizeResolverProvider.create)
    }

  private val maybePartitionSize: Option[KafkaPartitionSize] = maybeKafkaPartitionSizeResolver.map(_.create)

  private val passThroughMode = kafkaWriteAdaptorConfig.passThrough

  private val producerSettings: ProducerSettings[String, Array[Byte]] =
    ProducerSettings(
      kafkaWriteAdaptorConfig.sourceConfig.getConfig("producer"),
      new StringSerializer,
      new ByteArraySerializer
    )

  private def createHeaders(journal: JournalRow): util.List[Header] = {
    val pidHeader: Header =
      new RecordHeader(PersistenceIdHeaderKey, journal.persistenceId.asString.getBytes(StandardCharsets.UTF_8))
    val seqNrHeader: Header =
      new RecordHeader(SequenceNumberHeaderKey, journal.sequenceNumber.asString.getBytes(StandardCharsets.UTF_8))
    val deletedHeader: Header =
      new RecordHeader(DeletedHeaderKey, journal.deleted.toString.getBytes(StandardCharsets.UTF_8))
    val orderingHeader: Header =
      new RecordHeader(OrderingHeaderKey, journal.ordering.toString.getBytes(StandardCharsets.UTF_8))
    val headers: util.List[Header] = List(pidHeader, seqNrHeader, deletedHeader, orderingHeader).asJava
    headers
  }

  override def singlePutJournalRowFlow: Flow[JournalRow, Long, NotUsed] = {
    if (passThroughMode) underlying.singlePutJournalRowFlow
    else {
      Flow[JournalRow]
        .map { row =>
          val record = convertToRecord(row)
          ProducerMessage.single(record)
        }.via(Producer.flexiFlow(producerSettings)).map { _ => 1L }
    }
  }

  private def convertToRecord(journalRow: JournalRow): ProducerRecord[String, Array[Byte]] = {
    val topic = kafkaTopicResolver.create(journalRow.persistenceId)
    (maybeKafkaPartitionKeyResolver, maybePartitionSize) match {
      case (Some(kafkaPartitionKeyResolver), Some(partitionSize)) =>
        val partitionKey =
          kafkaPartitionKeyResolver.create(journalRow.persistenceId, journalRow.sequenceNumber, partitionSize)
        new ProducerRecord(
          topic.value,
          partitionKey.value,
          journalRow.persistenceId.asString,
          journalRow.message,
          createHeaders(journalRow)
        )
      case _ =>
        new ProducerRecord(
          topic.value,
          null,
          journalRow.persistenceId.asString,
          journalRow.message,
          createHeaders(journalRow)
        )
    }
  }

  override def multiPutJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] =
    if (passThroughMode) underlying.multiPutJournalRowsFlow
    else {
      Flow[Seq[JournalRow]]
        .map { rows =>
          val records = rows.map(convertToRecord).toVector
          ProducerMessage.multi(records)
        }.via(Producer.flexiFlow(producerSettings)).map {
          case ProducerMessage.MultiResult(parts, _) => parts.size
        }
    }

  override def singleDeleteJournalRowFlow: Flow[PersistenceIdWithSeqNr, Long, NotUsed] =
    underlying.singleDeleteJournalRowFlow

  override def multiDeleteJournalRowsFlow: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    underlying.multiDeleteJournalRowsFlow

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = underlying.updateMessage(journalRow)

  override def getJournalRows(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[JournalRow, NotUsed] =
    underlying.getJournalRows(persistenceId, fromSequenceNr, toSequenceNr, max, deleted)

  override def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): Source[Seq[JournalRow], NotUsed] =
    underlying.getJournalRows(persistenceId, toSequenceNr, deleted)

  override def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber],
      deleted: Option[Boolean]
  ): Source[Long, NotUsed] =
    underlying.highestSequenceNr(persistenceId, fromSequenceNr, deleted)

}
