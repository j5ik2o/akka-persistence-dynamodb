package com.github.j5ik2o.akka.persistence.dynamodb.config.client

object RetryMode extends Enumeration {
  val LEGACY, STANDARD, ADAPTIVE = Value
}
