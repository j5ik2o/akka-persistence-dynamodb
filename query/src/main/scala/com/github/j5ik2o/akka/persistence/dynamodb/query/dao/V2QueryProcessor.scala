package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import java.io.IOException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Concat, Flow, RestartFlow, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.StreamReadClient
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._
import software.amazon.awssdk.services.dynamodb.model.{ ScanRequest, Select, _ }
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

class V2QueryProcessor(
    system: ActorSystem,
    asyncClient: Option[JavaDynamoDbAsyncClient],
    syncClient: Option[JavaDynamoDbSyncClient],
    pluginConfig: QueryPluginConfig,
    metricsReporter: Option[MetricsReporter]
) extends QueryProcessor {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig

  private val streamClient =
    new StreamReadClient(system, asyncClient, syncClient, pluginConfig, pluginConfig.readBackoffConfig)

  override def allPersistenceIds(max: Long): Source[PersistenceId, NotUsed] = {
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = {
      val scanRequest = ScanRequest
        .builder()
        .tableName(pluginConfig.tableName)
        .select(Select.SPECIFIC_ATTRIBUTES)
        .attributesToGet(columnsDefConfig.deletedColumnName, columnsDefConfig.persistenceIdColumnName)
        .limit(pluginConfig.scanBatchSize)
        .exclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
        .consistentRead(pluginConfig.consistentRead)
        .build()
      Source.single(scanRequest).via(streamClient.scanFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          val items            = Option(response.items).map(_.asScala.toVector).map(_.map(_.asScala.toMap)).getOrElse(Vector.empty)
          val lastEvaluatedKey = Option(response.lastEvaluatedKey).map(_.asScala.toMap).getOrElse(Map.empty)
          val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
          if (lastEvaluatedKey.nonEmpty && (count + response.count()) < max) {
            loop(Some(lastEvaluatedKey), combinedSource, count + response.count(), index + 1)
          } else
            combinedSource
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }
    }

    loop(None, Source.empty, 0L, 1)
      .filterNot(_(columnsDefConfig.deletedColumnName).bool)
      .map(_(columnsDefConfig.persistenceIdColumnName).s)
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
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = {
      val scanRequest = ScanRequest
        .builder()
        .tableName(pluginConfig.tableName)
        .indexName(pluginConfig.tagsIndexName)
        .filterExpression("contains(#tags, :tag)")
        .expressionAttributeNames(
          Map("#tags" -> columnsDefConfig.tagsColumnName).asJava
        )
        .expressionAttributeValues(
          Map(
            ":tag" -> AttributeValue.builder().s(tag).build()
          ).asJava
        )
        .limit(pluginConfig.scanBatchSize)
        .exclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
        .build()
      Source
        .single(scanRequest).via(streamClient.scanFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val items =
              Option(response.items).map(_.asScala.toVector).map(_.map(_.asScala.toMap)).getOrElse(Vector.empty)
            val lastEvaluatedKey = Option(response.lastEvaluatedKey).map(_.asScala.toMap).getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty) {
              loop(Some(lastEvaluatedKey), combinedSource, count + response.count(), index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
          }
        }
    }

    loop(None, Source.empty, 0L, 1)
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
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = {
      val scanRequest = ScanRequest
        .builder().tableName(pluginConfig.tableName).select(Select.SPECIFIC_ATTRIBUTES).attributesToGet(
          columnsDefConfig.orderingColumnName
        ).limit(pluginConfig.scanBatchSize).exclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
        .consistentRead(pluginConfig.consistentRead)
        .build()
      Source
        .single(scanRequest)
        .via(streamClient.scanFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val items =
              Option(response.items).map(_.asScala.toVector).map(_.map(_.asScala.toMap)).getOrElse(Vector.empty)
            val lastEvaluatedKey = Option(response.lastEvaluatedKey).map(_.asScala.toMap).getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty) {
              loop(Some(lastEvaluatedKey), combinedSource, count + response.count(), index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
          }
        }
    }
    loop(None, Source.empty, 0L, 1)
      .map { result => result(columnsDefConfig.orderingColumnName).n.toLong }
      .drop(offset)
      .take(limit)
  }

  protected def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = PersistenceId(map(columnsDefConfig.persistenceIdColumnName).s),
      sequenceNumber = SequenceNumber(map(columnsDefConfig.sequenceNrColumnName).n.toLong),
      deleted = map(columnsDefConfig.deletedColumnName).bool,
      message = map.get(columnsDefConfig.messageColumnName).map(_.b.asByteArray()).get,
      ordering = map(columnsDefConfig.orderingColumnName).n.toLong,
      tags = map.get(columnsDefConfig.tagsColumnName).map(_.s)
    )
  }
}
