package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.serialization.{ Serialization, Serializers }

import scala.util.Try

final case class AkkaSerialized(serializerId: Int, serializerManifest: Option[String], payload: Array[Byte])

final class AkkaSerialization(serialization: Serialization) {

  def serialize(payload: Any): Try[AkkaSerialized] = {
    val p2          = payload.asInstanceOf[AnyRef]
    val serializer  = serialization.findSerializerFor(p2)
    val serManifest = Serializers.manifestFor(serializer, p2)
    val serialized  = serialization.serialize(p2)
    serialized.map { payload =>
      AkkaSerialized(serializer.identifier, if (serManifest.isEmpty) None else Some(serManifest), payload)
    }
  }

  def deserialize(serialized: AkkaSerialized): Try[AnyRef] = {
    serialization.deserialize(serialized.payload, serialized.serializerId, serialized.serializerManifest.getOrElse(""))
  }

}
