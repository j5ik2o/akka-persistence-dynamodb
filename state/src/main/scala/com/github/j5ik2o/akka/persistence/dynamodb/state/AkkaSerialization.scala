package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.serialization.{ AsyncSerializer, Serialization, Serializer, Serializers }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

final case class AkkaSerialized(serializerId: Int, serializerManifest: Option[String], payload: Array[Byte])

final class AkkaSerialization(serialization: Serialization) {

  def serialize(payload: Any)(implicit ec: ExecutionContext): Future[AkkaSerialized] = {
    val p2 = payload.asInstanceOf[AnyRef]
    for {
      serializer <- serializerAsync(p2)
      serManifest <- Future.successful(Serializers.manifestFor(serializer, p2))
      payload <- toBinaryAsync(serializer, p2)
    } yield AkkaSerialized(serializer.identifier, if (serManifest.isEmpty) None else Some(serManifest), payload)
  }

  def deserialize(serialized: AkkaSerialized)(implicit ec: ExecutionContext): Future[AnyRef] = {
    for {
      serializer <- serializerAsync(serialized.payload)
      result <- fromBinaryAsync(serializer, serialized)
    } yield result
  }

  private def serializerAsync(payload: AnyRef): Future[Serializer] = {
    try Future.successful(serialization.findSerializerFor(payload))
    catch {
      case ex: Throwable =>
        Future.failed(ex)
    }
  }

  private def toBinaryAsync(serializer: Serializer, payload: AnyRef)(implicit
      ec: ExecutionContext
  ): Future[Array[Byte]] = {
    serializer match {
      case async: AsyncSerializer => async.toBinaryAsync(payload)
      case _ =>
        serialization.serialize(payload) match {
          case Success(value) => Future.successful(value)
          case Failure(ex)    => Future.failed(ex)
        }
    }
  }

  private def fromBinaryAsync(serializer: Serializer, serialized: AkkaSerialized)(implicit
      ec: ExecutionContext
  ): Future[AnyRef] = {
    val future = serializer match {
      case async: AsyncSerializer =>
        async.fromBinaryAsync(serialized.payload, serialized.serializerManifest.getOrElse(""))
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
