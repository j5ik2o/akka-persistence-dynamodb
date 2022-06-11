package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.serialization.{ AsyncSerializer, Serialization, Serializer, Serializers }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

final class StateSerializer(
    serialization: Serialization,
    metricsReporter: Option[MetricsReporter],
    traceReporter: Option[TraceReporter]
) {

  def serialize(persistenceId: String, payload: Any)(implicit ec: ExecutionContext): Future[AkkaSerialized] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreSerializeState(context))

    def future: Future[AkkaSerialized] = {
      val p2 = payload.asInstanceOf[AnyRef]
      for {
        serializer <- serializerAsync(p2)
        serializerManifest <- Future.successful(Serializers.manifestFor(serializer, p2))
        payload <- toBinaryAsync(serializer, p2)
      } yield AkkaSerialized(
        serializer.identifier,
        if (serializerManifest.isEmpty) None else Some(serializerManifest),
        payload
      )
    }

    val traced = traceReporter.fold(future)(_.traceStateStoreSerializeState(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterStateStoreSerializeState(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorStateStoreSerializeState(newContext, ex))
    }

    traced
  }

  def deserialize(persistenceId: String, serialized: AkkaSerialized)(implicit ec: ExecutionContext): Future[AnyRef] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreDeserializeState(context))

    def future: Future[AnyRef] = for {
      serializer <- serializerAsync(serialized.payload)
      result <- fromBinaryAsync(serializer, serialized)
    } yield result

    val traced = traceReporter.fold(future)(_.traceStateStoreDeserializeState(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterStateStoreDeserializeState(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorStateStoreDeserializeState(newContext, ex))
    }

    traced
  }

  private def serializerAsync(payload: AnyRef): Future[Serializer] = {
    try Future.successful(serialization.findSerializerFor(payload))
    catch {
      case NonFatal(ex) =>
        Future.failed(ex)
    }
  }

  private def toBinaryAsync(serializer: Serializer, payload: AnyRef): Future[Array[Byte]] = {
    serializer match {
      case asyncSerializer: AsyncSerializer => asyncSerializer.toBinaryAsync(payload)
      case _ =>
        serialization.serialize(payload) match {
          case Success(value) => Future.successful(value)
          case Failure(ex)    => Future.failed(ex)
        }
    }
  }

  private def fromBinaryAsync(serializer: Serializer, serialized: AkkaSerialized): Future[AnyRef] = {
    val future = serializer match {
      case asyncSerializer: AsyncSerializer =>
        asyncSerializer.fromBinaryAsync(serialized.payload, serialized.serializerManifest.getOrElse(""))
      case _ =>
        serialization.deserialize(
          serialized.payload,
          serialized.serializerId,
          serialized.serializerManifest.getOrElse("")
        ) match {
          case Success(value) => Future.successful(value)
          case Failure(ex)    => Future.failed(ex)
        }
    }
    future
  }

}
