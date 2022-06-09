package com.github.j5ik2o.akka.persistence.dynamodb.state

final case class GetRawObjectResult[A](value: Option[A], revision: Long, tag: Option[String])
