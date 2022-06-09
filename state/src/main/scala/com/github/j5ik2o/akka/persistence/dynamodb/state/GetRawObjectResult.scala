package com.github.j5ik2o.akka.persistence.dynamodb.state

sealed trait GetRawObjectResult[+A]

object GetRawObjectResult {
  final case class Just[A](
      pkey: String,
      persistenceId: String,
      value: Option[A],
      revision: Long,
      serializerId: Int,
      serializerManifest: Option[String],
      tag: Option[String],
      ordering: Long
  ) extends GetRawObjectResult[A]
  final case object Empty extends GetRawObjectResult[Nothing]
}
