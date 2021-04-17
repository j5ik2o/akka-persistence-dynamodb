package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, ScanRequest, Select }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.StreamReadClient
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class V1QueryProcessor(
    system: ActorSystem,
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: QueryPluginConfig,
    val metricsReporter: Option[MetricsReporter]
)(implicit ec: ExecutionContext)
    extends QueryProcessor {

  private val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig

  private val streamClient =
    new StreamReadClient(system, asyncClient, syncClient, pluginConfig, pluginConfig.readBackoffConfig)

  override def allPersistenceIds(max: Long): Source[PersistenceId, NotUsed] = {
    val scanRequest = new ScanRequest()
      .withTableName(pluginConfig.tableName)
      .withSelect(Select.SPECIFIC_ATTRIBUTES)
      .withAttributesToGet(columnsDefConfig.deletedColumnName, columnsDefConfig.persistenceIdColumnName)
      .withLimit(pluginConfig.scanBatchSize)
      .withConsistentRead(pluginConfig.consistentRead)
    streamClient
      .recursiveScanSource(scanRequest, Some(max)).mapConcat { result =>
        Option(result.getItems).map(_.asScala.toVector).map(_.map(_.asScala.toMap)).getOrElse(Vector.empty)
      }
      .filterNot(_(columnsDefConfig.deletedColumnName).getBOOL)
      .map(_(columnsDefConfig.persistenceIdColumnName).getS)
      .fold(Set.empty[String])(_ + _)
      .mapConcat(_.toVector)
      .map(PersistenceId.apply)
      .take(max)
      .withAttributes(logLevels)
  }

  override def eventsByTagAsJournalRow(
      tag: String,
      offset: Long,
      maxOffset: Long,
      max: Long
  ): Source[JournalRow, NotUsed] = {
    val scanRequest = new ScanRequest()
      .withTableName(pluginConfig.tableName)
      .withIndexName(pluginConfig.tagsIndexName)
      .withFilterExpression("contains(#tags, :tag)")
      .withExpressionAttributeNames(
        Map("#tags" -> columnsDefConfig.tagsColumnName).asJava
      )
      .withExpressionAttributeValues(
        Map(
          ":tag" -> new AttributeValue().withS(tag)
        ).asJava
      )
      .withLimit(pluginConfig.scanBatchSize)
    streamClient
      .recursiveScanSource(scanRequest, Some(max)).mapConcat { result =>
        Option(result.getItems).map(_.asScala.toVector).map(_.map(_.asScala.toMap)).getOrElse(Vector.empty)
      }
      .map(convertToJournalRow)
      .fold(ArrayBuffer.empty[JournalRow])(_ += _)
      .map(_.sortBy(journalRow => (journalRow.persistenceId.asString, journalRow.sequenceNumber.value)))
      .mapConcat(_.toVector)
      .statefulMapConcat { () =>
        val index = new AtomicLong()
        journalRow => List(journalRow.withOrdering(index.incrementAndGet()))
      }
      .filter(journalRow => journalRow.ordering > offset && journalRow.ordering <= maxOffset)
      .take(max)
      .withAttributes(logLevels)
  }

  override def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed] = {
    val scanRequest = new ScanRequest()
      .withTableName(pluginConfig.tableName).withSelect(Select.SPECIFIC_ATTRIBUTES).withAttributesToGet(
        columnsDefConfig.orderingColumnName
      ).withLimit(pluginConfig.scanBatchSize)
      .withConsistentRead(pluginConfig.consistentRead)
    streamClient
      .recursiveScanSource(scanRequest, None).mapConcat { result =>
        Option(result.getItems).map(_.asScala.toVector).map(_.map(_.asScala.toMap)).getOrElse(Vector.empty)
      }
      .map { result => result(columnsDefConfig.orderingColumnName).getN.toLong }
      .drop(offset)
      .take(limit)
  }

  protected def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = PersistenceId(map(columnsDefConfig.persistenceIdColumnName).getS),
      sequenceNumber = SequenceNumber(map(columnsDefConfig.sequenceNrColumnName).getN.toLong),
      deleted = map(columnsDefConfig.deletedColumnName).getBOOL,
      message = map.get(columnsDefConfig.messageColumnName).map(_.getB.array()).get,
      ordering = map(columnsDefConfig.orderingColumnName).getN.toLong,
      tags = map.get(columnsDefConfig.tagsColumnName).map(_.getS)
    )
  }
}
