package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.scaladsl.{ Concat, Flow, RestartFlow, Source }
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, ScanRequest, ScanResult, Select }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, Stopwatch }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.akka.persistence.dynamodb.utils.JavaFutureConverter._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class V1QueryProcessor(
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: QueryPluginConfig,
    val metricsReporter: Option[MetricsReporter]
)(implicit ec: ExecutionContext)
    extends QueryProcessor {

  val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig

  private def scanFlow: Flow[ScanRequest, ScanResult, NotUsed] = {
    val flow = ((asyncClient, syncClient) match {
      case (None, Some(c)) =>
        val flow = Flow[ScanRequest].map { request =>
          val sw     = Stopwatch.start()
          val result = c.scan(request)
          metricsReporter.foreach(_.setDynamoDBClientScanDuration(sw.elapsed()))
          result
        }
        DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
      case (Some(c), None) =>
        Flow[ScanRequest].mapAsync(1) { request =>
          val sw     = Stopwatch.start()
          val future = c.scanAsync(request).toScala
          future.onComplete { _ => metricsReporter.foreach(_.setDynamoDBClientScanDuration(sw.elapsed())) }
          future
        }
      case _ =>
        throw new IllegalStateException("invalid state")
    }).log("scan")
    if (pluginConfig.readBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = pluginConfig.readBackoffConfig.minBackoff,
          maxBackoff = pluginConfig.readBackoffConfig.maxBackoff,
          randomFactor = pluginConfig.readBackoffConfig.randomFactor,
          maxRestarts = pluginConfig.readBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

  override def allPersistenceIds(max: Long): Source[PersistenceId, NotUsed] = {
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = {
      val scanRequest = new ScanRequest()
        .withTableName(pluginConfig.tableName)
        .withSelect(Select.SPECIFIC_ATTRIBUTES)
        .withAttributesToGet(columnsDefConfig.deletedColumnName, columnsDefConfig.persistenceIdColumnName)
        .withLimit(pluginConfig.scanBatchSize)
        .withExclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
        .withConsistentRead(pluginConfig.consistentRead)
      Source.single(scanRequest).via(scanFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
          val items =
            Option(response.getItems).map(_.asScala.map(_.asScala.toMap)).getOrElse(Seq.empty).toVector
          val lastEvaluatedKey = Option(response.getLastEvaluatedKey).map(_.asScala.toMap)
          val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
          if (lastEvaluatedKey.nonEmpty && (count + response.getCount) < max) {
            loop(lastEvaluatedKey, combinedSource, count + response.getCount, index + 1)
          } else
            combinedSource
        } else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }
    }

    loop(None, Source.empty, 0L, 1)
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
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = {
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
        .withExclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
      Source
        .single(scanRequest).via(scanFlow).flatMapConcat { response =>
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            val items =
              Option(response.getItems).map(_.asScala.map(_.asScala.toMap)).getOrElse(Seq.empty).toVector
            val lastEvaluatedKey = Option(response.getLastEvaluatedKey).map(_.asScala.toMap)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty) {
              loop(lastEvaluatedKey, combinedSource, count + response.getCount, index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
            Source.failed(new IOException(s"statusCode: $statusCode"))
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
    ): Source[Map[String, AttributeValue], NotUsed] = startTimeSource.flatMapConcat { requestStart =>
      val scanRequest = new ScanRequest()
        .withTableName(pluginConfig.tableName).withSelect(Select.SPECIFIC_ATTRIBUTES).withAttributesToGet(
          columnsDefConfig.orderingColumnName
        ).withLimit(pluginConfig.scanBatchSize).withExclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
        .withConsistentRead(pluginConfig.consistentRead)
      Source
        .single(scanRequest)
        .via(scanFlow).flatMapConcat { response =>
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            val items =
              Option(response.getItems).map(_.asScala.map(_.asScala.toMap)).getOrElse(Seq.empty).toVector
            val lastEvaluatedKey = Option(response.getLastEvaluatedKey).map(_.asScala.toMap)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty) {
              loop(lastEvaluatedKey, combinedSource, count + response.getCount, index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
            Source.failed(new IOException(s"statusCode: $statusCode"))
          }
        }
    }
    loop(None, Source.empty, 0L, 1)
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
