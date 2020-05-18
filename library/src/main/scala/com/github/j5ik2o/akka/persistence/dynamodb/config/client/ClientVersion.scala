package com.github.j5ik2o.akka.persistence.dynamodb.config.client

object ClientVersion extends Enumeration {
  val V1: ClientVersion.Value    = Value("v1")
  val V1Dax: ClientVersion.Value = Value("v1-dax")
  val V2: ClientVersion.Value    = Value("v2")
}
