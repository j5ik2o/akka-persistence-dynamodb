package com.github.j5ik2o.akka.persistence.dynamodb.config.client

object ClientType extends Enumeration {
  val Sync: ClientType.Value  = Value("sync")
  val Async: ClientType.Value = Value("async")
}
