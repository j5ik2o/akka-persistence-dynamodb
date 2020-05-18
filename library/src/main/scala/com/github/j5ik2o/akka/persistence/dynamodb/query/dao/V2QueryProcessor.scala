package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Concat, Flow, GraphDSL, RestartFlow, Source, Unzip, Zip }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalColumnsDefConfig, QueryPluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, Stopwatch }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDbAsyncClient, DynamoDbSyncClient }
import software.amazon.awssdk.services.dynamodb.model.{ ScanRequest, Select, _ }

import scala.collection.mutable.ArrayBuffer

class V2QueryProcessor(
    asyncClient: Option[DynamoDbAsyncClient],
    syncClient: Option[DynamoDbSyncClient],
    pluginConfig: QueryPluginConfig,
    metricsReporter: MetricsReporter
) extends QueryProcessor {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  val columnsDefConfig: JournalColumnsDefConfig = pluginConfig.columnsDefConfig
  private val streamClient                      = asyncClient.map(DynamoDbAkkaClient(_))

  private def scanFlow: Flow[ScanRequest, ScanResponse, NotUsed] = {
    val flow = ((streamClient, syncClient) match {
      case (None, Some(c)) =>
        val flow = Flow[ScanRequest].map { request =>
          val sw     = Stopwatch.start()
          val result = c.scan(request)
          metricsReporter.setScanDuration(sw.elapsed())
          result match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case (Some(c), None) =>
        Flow[ScanRequest].flatMapConcat { request =>
          Source
            .single((request, Stopwatch.start())).via(Flow.fromGraph(GraphDSL.create() { implicit b =>
              import GraphDSL.Implicits._
              val unzip = b.add(Unzip[ScanRequest, Stopwatch]())
              val zip   = b.add(Zip[ScanResponse, Stopwatch]())
              unzip.out0 ~> c.scanFlow(1) ~> zip.in0
              unzip.out1 ~> zip.in1
              FlowShape(unzip.in, zip.out)
            })).map {
              case (response, sw) =>
                metricsReporter.setScanDuration(sw.elapsed())
                response
            }
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
      val scanRequest = ScanRequest
        .builder()
        .tableName(pluginConfig.tableName)
        .select(Select.SPECIFIC_ATTRIBUTES)
        .attributesToGet(columnsDefConfig.deletedColumnName, columnsDefConfig.persistenceIdColumnName)
        .limit(pluginConfig.scanBatchSize)
        .exclusiveStartKeyAsScala(lastEvaluatedKey)
        .consistentRead(pluginConfig.consistentRead)
        .build()
      Source.single(scanRequest).via(scanFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
          val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
          val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
          if (lastEvaluatedKey.nonEmpty && (count + response.count()) < max) {
            loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
          } else
            combinedSource
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
        }
      }
    }

    loop(None, Source.empty, 0L, 1)
      .filterNot(_(columnsDefConfig.deletedColumnName).bool.get)
      .map(_(columnsDefConfig.persistenceIdColumnName).s.get)
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
        .expressionAttributeNamesAsScala(
          Map("#tags" -> columnsDefConfig.tagsColumnName)
        )
        .expressionAttributeValuesAsScala(
          Map(
            ":tag" -> AttributeValue.builder().s(tag).build()
          )
        )
        .limit(pluginConfig.scanBatchSize)
        .exclusiveStartKeyAsScala(lastEvaluatedKey)
        .build()
      Source
        .single(scanRequest).via(scanFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
            val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty) {
              loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
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
        ).limit(pluginConfig.scanBatchSize).exclusiveStartKeyAsScala(lastEvaluatedKey)
        .consistentRead(pluginConfig.consistentRead)
        .build()
      Source
        .single(scanRequest)
        .via(scanFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
            val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty) {
              loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
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
      deleted = map(columnsDefConfig.deletedColumnName).bool.get,
      message = map.get(columnsDefConfig.messageColumnName).map(_.b.asByteArray()).get,
      ordering = map(columnsDefConfig.orderingColumnName).n.toLong,
      tags = map.get(columnsDefConfig.tagsColumnName).map(_.s)
    )
  }
}
