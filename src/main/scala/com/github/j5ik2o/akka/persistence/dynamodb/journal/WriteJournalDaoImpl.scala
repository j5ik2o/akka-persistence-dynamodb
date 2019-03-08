package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.NotUsed
import akka.serialization.Serialization
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.{ Attributes, Materializer, OverflowStrategy, QueueOfferResult }
import com.github.j5ik2o.akka.persistence.dynamodb.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.config.PersistencePluginConfig
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDBStreamClient
import com.github.j5ik2o.reactive.aws.dynamodb.model._
import com.github.j5ik2o.reactive.aws.dynamodb.monix.DynamoDBTaskClientV2
import monix.eval.Task
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future, Promise }

class WriteJournalDaoImpl(asyncClient: DynamoDBAsyncClientV2,
                          serialization: Serialization,
                          persistencePluginConfig: PersistencePluginConfig)(
    implicit ec: ExecutionContext,
    mat: Materializer
) extends WriteJournalDaoWithUpdates {

  import com.github.j5ik2o.akka.persistence.dynamodb.Columns._

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val scheduler: Scheduler = Scheduler(ec)

  private val tableName: String = persistencePluginConfig.journalTableName
  private val bufferSize: Int   = persistencePluginConfig.bufferSize
  private val batchSize: Int    = persistencePluginConfig.batchSize
  private val parallelism: Int  = persistencePluginConfig.parallelism

  private val taskClient: DynamoDBTaskClientV2   = DynamoDBTaskClientV2(asyncClient)
  private val streamClient: DynamoDBStreamClient = DynamoDBStreamClient(asyncClient)

  private val logLevels = Attributes.logLevels(onElement = Attributes.LogLevels.Info,
                                               onFailure = Attributes.LogLevels.Error,
                                               onFinish = Attributes.LogLevels.Info)
  private val putQueue = Source
    .queue[(Promise[Long], Seq[JournalRow])](bufferSize, OverflowStrategy.dropNew)
    .flatMapConcat {
      case (promise, rows) =>
        Source(rows.toVector)
          .grouped(batchSize).log("grouped")
          .via(putJournalRowsFlow).log("result")
          .async
          .fold(ArrayBuffer.empty[Long])(_ :+ _).log("results")
          .map(_.sum).log("sum")
          .map(result => promise.success(result))
          .recover { case t => promise.failure(t) }
    }
    .toMat(Sink.ignore)(Keep.left)
    .withAttributes(logLevels)
    .run()

  private val deleteQueue = Source
    .queue[(Promise[Long], Seq[PersistenceIdWithSeqNr])](bufferSize, OverflowStrategy.dropNew)
    .flatMapConcat {
      case (promise, rows) =>
        Source(rows.toVector)
          .grouped(batchSize).log("grouped")
          .via(deleteJournalRowsFlow).log("result")
          .async
          .fold(ArrayBuffer.empty[Long])(_ :+ _).log("results")
          .map(_.sum).log("sum")
          .map(result => promise.success(result))
          .recover { case t => promise.failure(t) }
    }
    .toMat(Sink.ignore)(Keep.left)
    .withAttributes(logLevels)
    .run()

  private def _putJournalRows: Flow[Seq[JournalRow], Long, NotUsed] =
    Flow[Seq[JournalRow]].mapAsync(parallelism) { messages =>
      val promise = Promise[Long]()
      putQueue.offer(promise -> messages).flatMap {
        case QueueOfferResult.Enqueued =>
          promise.future
        case QueueOfferResult.Failure(t) =>
          Future.failed(new Exception("Failed to write journal row batch", t))
        case QueueOfferResult.Dropped =>
          Future.failed(
            new Exception(
              s"Failed to enqueue journal row batch write, the queue buffer was full ($bufferSize elements) please check the jdbc-journal.bufferSize setting"
            )
          )
        case QueueOfferResult.QueueClosed =>
          Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
      }
    }

  private def _deleteJournalRows: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]].mapAsync(parallelism) { messages =>
      val promise = Promise[Long]()
      deleteQueue.offer(promise -> messages).flatMap {
        case QueueOfferResult.Enqueued =>
          promise.future
        case QueueOfferResult.Failure(t) =>
          Future.failed(new Exception("Failed to write journal row batch", t))
        case QueueOfferResult.Dropped =>
          Future.failed(
            new Exception(
              s"Failed to enqueue journal row batch write, the queue buffer was full ($bufferSize elements) please check the jdbc-journal.bufferSize setting"
            )
          )
        case QueueOfferResult.QueueClosed =>
          Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
      }
    }

  override def putMessages(messages: Seq[JournalRow]): Source[Long, NotUsed] =
    Source.single(messages).via(_putJournalRows)

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    val updateRequest = UpdateItemRequest()
      .withTableName(Some(tableName)).withKey(
        Some(
          Map(
            PersistenceIdColumnName -> AttributeValue().withString(Some(journalRow.persistenceId.toString)),
            SequenceNrColumnName    -> AttributeValue().withNumber(Some(journalRow.sequenceNumber.toString))
          )
        )
      ).withAttributeUpdates(
        Some(
          Map(
            MessageColumnName -> AttributeValueUpdate()
              .withAction(Some(AttributeAction.PUT)).withValue(
                Some(AttributeValue().withBinary(Some(journalRow.message)))
              ),
            OrderingColumnName ->
            AttributeValueUpdate()
              .withAction(Some(AttributeAction.PUT)).withValue(
                Some(
                  AttributeValue().withNumber(Some(journalRow.ordering.toString))
                )
              ),
            DeletedColumnName -> AttributeValueUpdate()
              .withAction(Some(AttributeAction.PUT)).withValue(
                Some(AttributeValue().withBool(Some(journalRow.deleted))),
              )
          ) ++ journalRow.tags
            .map { t =>
              Map(
                TagsColumnName -> AttributeValueUpdate()
                  .withAction(Some(AttributeAction.PUT)).withValue(Some(AttributeValue().withString(Some(t))))
              )
            }.getOrElse(Map.empty)
        )
      )
    Source.single(updateRequest).via(streamClient.updateItemFlow(parallelism)).map { result =>
      if (result.isSuccessful)
        ()
      else
        throw new Exception()
    }
  }

  private def getJournalRows(persistenceId: String,
                             toSequenceNr: Long,
                             deleted: Boolean = false): Task[Seq[JournalRow]] = {
    val result = taskClient
      .query(
        QueryRequest()
          .withTableName(Some(tableName))
          .withKeyConditionExpression(Some("#pid = :pid and #snr <= :snr"))
          .withFilterExpression(Some("#d = :flg"))
          .withExpressionAttributeNames(
            Some(Map("#pid" -> PersistenceIdColumnName, "#snr" -> SequenceNrColumnName, "#d" -> DeletedColumnName))
          )
          .withExpressionAttributeValues(
            Some(
              Map(
                ":pid" -> AttributeValue().withString(Some(persistenceId)),
                ":snr" -> AttributeValue().withNumber(Some(toSequenceNr.toString)),
                ":flg" -> AttributeValue().withBool(Some(deleted))
              )
            )
          )
      ).flatMap { result =>
        if (result.isSuccessful)
          Task.pure(result.items.get)
        else
          Task.raiseError(new Exception)
      }.map(convertToJournalRows).map { result =>
        logger.debug(s"getJournalRows = $result")
        result
      }
    result
  }

  private def updateJournalRows(journalRows: Seq[JournalRow]): Task[Long] = Task.deferFuture {
    val f = putMessages(journalRows).runWith(Sink.head)
    f
  }

  private def getHighestMarkedSequenceNr(persistenceId: String): Task[Option[Long]] = Task.deferFuture {
    _highestSequenceNr(persistenceId, deleted = Some(true)).runWith(Sink.headOption)
  }

  private def deleteBy(persistenceId: String, sequenceNrs: Seq[Long]): Task[Unit] = {
    logger.debug(s"deleteBy($persistenceId, $sequenceNrs): start")
    if (sequenceNrs.isEmpty)
      Task.pure(())
    else
      Task.deferFuture {
        Source
          .single(sequenceNrs.map(snr => PersistenceIdWithSeqNr(persistenceId, snr))).via(_deleteJournalRows).runWith(
            Sink.ignore
          ).map(_ => ())
      }
  }

  /**
    * val actions = for {
    * _ <- * JournalTable
    *  .filter(_.persistenceId === persistenceId)
    *  .filter(_.sequenceNumber <= maxSequenceNr)
    *  .filter(_.deleted === false)
    *  .map(_.deleted).update(true)
    *
    * highestMarkedSequenceNr <- JournalTable.filter(_.persistenceId === persistenceId)
    *   .filter(_.deleted === true).sortBy(_.sequenceNumber.desc).map(_.sequenceNumber)
    *
    * _ <- JournalTable.filter(_.persistenceId === persistenceId)
    *   .filter(_.sequenceNumber <= highestMarkedSequenceNr.getOrElse(0L) - 1)
    *   .delete
    *
    * } yield ()
    *
    * @param persistenceId
    * @param maxSequenceNr
    * @return
    */
  override def deleteMessages(persistenceId: String, toSequenceNr: Long): Source[Unit, NotUsed] = {
    Source.fromFuture((for {
      results                 <- getJournalRows(persistenceId, toSequenceNr)
      _                       <- updateJournalRows(results.map(_.withDeleted))
      highestMarkedSequenceNr <- getHighestMarkedSequenceNr(persistenceId)
      journalRows             <- getJournalRows(persistenceId, highestMarkedSequenceNr.getOrElse(0L) - 1)
      _                       <- deleteBy(persistenceId, journalRows.map(_.sequenceNumber))
    } yield ()).runToFuture)
  }

  def _highestSequenceNr(persistenceId: String,
                         fromSequenceNr: Option[Long] = None,
                         deleted: Option[Boolean] = None): Source[Long, NotUsed] = {
    val queryRequest = QueryRequest()
      .withTableName(Some(tableName))
      .withKeyConditionExpression(
        fromSequenceNr.map(_ => "#pid = :id and #snr >= :nr").orElse(Some("#pid = :id"))
      )
      .withFilterExpression(deleted.map(_ => "#d = :flg"))
      .withExpressionAttributeNames(
        Some(
          Map(
            "#pid" -> PersistenceIdColumnName,
          ) ++ deleted
            .map { _ =>
              Map("#d" -> DeletedColumnName)
            }.getOrElse(Map.empty) ++ fromSequenceNr
            .map { _ =>
              Map(
                "#snr" -> SequenceNrColumnName
              )
            }.getOrElse(Map.empty)
        )
      )
      .withExpressionAttributeValues(
        Some(
          Map(
            ":id" -> AttributeValue().withString(Some(persistenceId)),
          ) ++ deleted
            .map { d =>
              Map(
                ":flg" -> AttributeValue().withBool(Some(d))
              )
            }.getOrElse(Map.empty) ++ fromSequenceNr
            .map { nr =>
              Map(
                ":nr" -> AttributeValue().withNumber(Some(nr.toString))
              )
            }.getOrElse(Map.empty)
        )
      ).withScanIndexForward(Some(false))
      .withLimit(Some(1))
    Source
      .single(queryRequest).via(streamClient.queryFlow()).map {
        _.items.get.map(_(SequenceNrColumnName).number.get.toLong).headOption.getOrElse(0)
      }

  }

  override def highestSequenceNr(persistenceId: String, fromSequenceNr: Long): Source[Long, NotUsed] = {
    _highestSequenceNr(persistenceId, Some(fromSequenceNr))
  }

  override def getMessages(persistenceId: String,
                           fromSequenceNr: Long,
                           toSequenceNr: Long,
                           max: Long,
                           deleted: Option[Boolean] = Some(false)): Source[JournalRow, NotUsed] = {
    Source
      .unfold(fromSequenceNr) {
        case nr if nr > toSequenceNr =>
          None
        case nr =>
          Some(nr + 1, nr)
      }
      .grouped(parallelism).log("grouped")
      .map { seqNos =>
        BatchGetItemRequest()
          .withRequestItems(
            Some(
              Map(
                tableName -> KeysAndAttributes()
                  .withKeys(
                    Some(
                      seqNos.map { seqNr =>
                        Map(PersistenceIdColumnName -> AttributeValue().withString(Some(persistenceId)),
                            SequenceNrColumnName    -> AttributeValue().withNumber(Some(seqNr.toString)))
                      }
                    )
                  )
              )
            )
          )
      }
      .via(streamClient.batchGetItemFlow())
      .map { batchGetItemResponse =>
        if (batchGetItemResponse.isSuccessful)
          batchGetItemResponse.responses.getOrElse(Map.empty)
        else
          throw new Exception
      }
      .mapConcat { result =>
        (deleted match {
          case None =>
            convertToJournalRows(result(tableName))
          case Some(b) =>
            convertToJournalRows(result(tableName)).filter(_.deleted == b)
        }).sortBy(_.sequenceNumber).toVector
      }.log("journalRow").withAttributes(logLevels)
      .take(max)

  }

  private def convertToJournalRows(values: Seq[Map[String, AttributeValue]]): Seq[JournalRow] = {
    values.map { map =>
      JournalRow(
        persistenceId = map(PersistenceIdColumnName).string.get,
        sequenceNumber = map(SequenceNrColumnName).number.get.toLong,
        deleted = map(DeletedColumnName).bool.get,
        message = {
          map.get(MessageColumnName).flatMap(_.binary).get

        },
        ordering = map(OrderingColumnName).number.get.toLong,
        tags = map.get(TagsColumnName).flatMap(_.string)
      )
    }
  }

  case class PersistenceIdWithSeqNr(persistenceId: String, sequenceNumber: Long)

  private def deleteJournalRowsFlow: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]].flatMapConcat { xs =>
      logger.debug(s"deleteJournalRows.size: ${xs.size}")
      logger.debug(s"deleteJournalRows: $xs")
      xs.map { case PersistenceIdWithSeqNr(pid, seqNr) => s"pid = $pid, seqNr = $seqNr" }.foreach(logger.debug)
      def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
        Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
          if (requestItems.isEmpty)
            Source.single(0L)
          else {
            Source
              .single(requestItems).map { requests =>
                BatchWriteItemRequest().withRequestItems(Some(Map(tableName -> requests)))
              }.via(streamClient.batchWriteItemFlow()).flatMapConcat { response =>
                if (response.isSuccessful) {
                  if (response.unprocessedItems.get.nonEmpty) {
                    val n = requestItems.size - response.unprocessedItems.get(tableName).size
                    Source.single(response.unprocessedItems.get(tableName)).via(loopFlow).map(_ + n)
                  } else
                    Source.single(requestItems.size)
                } else {
                  throw new Exception
                }
              }
          }
        }
      val requestItems = xs.map { x =>
        WriteRequest().withDeleteRequest(
          Some(
            DeleteRequest().withKey(
              Some(
                Map(
                  PersistenceIdColumnName -> AttributeValue().withString(Some(x.persistenceId)),
                  SequenceNrColumnName    -> AttributeValue().withNumber(Some(x.sequenceNumber.toString))
                )
              )
            )
          )
        )
      }
      Source.single(requestItems).via(loopFlow)
    }

  private def putJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] = Flow[Seq[JournalRow]].flatMapConcat { xs =>
    logger.info(s"putJournalRows.size: ${xs.size}")
    logger.info(s"putJournalRows: $xs")
    xs.map(_.persistenceId).map(p => s"pid = $p").foreach(logger.info)
    def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
      Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
        if (requestItems.isEmpty)
          Source.single(0L)
        else {
          Source
            .single(requestItems).map { requests =>
              BatchWriteItemRequest().withRequestItems(Some(Map(tableName -> requests)))
            }.via(streamClient.batchWriteItemFlow()).flatMapConcat { response =>
              if (response.isSuccessful) {
                if (response.unprocessedItems.get.nonEmpty) {
                  val n = requestItems.size - response.unprocessedItems.get(tableName).size
                  Source.single(response.unprocessedItems.get(tableName)).via(loopFlow).map(_ + n)
                } else
                  Source.single(requestItems.size)
              } else {
                throw new Exception
              }
            }
        }
      }
    val requestItems = xs.map { x =>
      WriteRequest().withPutRequest(
        Some(
          PutRequest()
            .withItem(
              Some(
                Map(
                  PersistenceIdColumnName -> AttributeValue().withString(Some(x.persistenceId)),
                  SequenceNrColumnName    -> AttributeValue().withNumber(Some(x.sequenceNumber.toString)),
                  OrderingColumnName      -> AttributeValue().withNumber(Some(x.ordering.toString)),
                  DeletedColumnName       -> AttributeValue().withBool(Some(x.deleted)),
                  MessageColumnName       -> AttributeValue().withBinary(Some(x.message))
                ) ++ x.tags
                  .map { t =>
                    Map(TagsColumnName -> AttributeValue().withString(Some(t)))
                  }.getOrElse(Map.empty)
              )
            )
        )
      )
    }
    Source.single(requestItems).via(loopFlow)
  }

}
