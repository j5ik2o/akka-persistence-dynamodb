package com.github.j5ik2o.akka.persistence.dynamodb.state

final case class AkkaSerialized(serializerId: Int, serializerManifest: Option[String], payload: Array[Byte])
