package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.typesafe.config.Config

import scala.concurrent.duration._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._

object JournalSequenceRetrievalConfig {

  def fromConfig(config: Config): JournalSequenceRetrievalConfig =
    JournalSequenceRetrievalConfig(
      batchSize = config.asInt("batch-size", 10000),
      maxTries = config.asInt("max-tries", 10),
      queryDelay = config.asFiniteDuration("query-delay", 1.second),
      maxBackoffQueryDelay = config.asFiniteDuration("max-backoff-query-delay", 1.minute),
      askTimeout = config.asFiniteDuration("ask-timeout", 1.second)
    )

}

case class JournalSequenceRetrievalConfig(batchSize: Int,
                                          maxTries: Int,
                                          queryDelay: FiniteDuration,
                                          maxBackoffQueryDelay: FiniteDuration,
                                          askTimeout: FiniteDuration)
