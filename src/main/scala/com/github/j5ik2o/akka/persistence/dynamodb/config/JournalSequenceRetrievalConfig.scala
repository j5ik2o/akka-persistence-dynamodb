package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.typesafe.config.Config

import scala.concurrent.duration._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._

object JournalSequenceRetrievalConfig {

  def apply(config: Config): JournalSequenceRetrievalConfig =
    JournalSequenceRetrievalConfig(
      batchSize = config.asInt("journal-sequence-retrieval.batch-size", 10000),
      maxTries = config.asInt("journal-sequence-retrieval.max-tries", 10),
      queryDelay = config.asFiniteDuration("journal-sequence-retrieval.query-delay", 1.second),
      maxBackoffQueryDelay = config.asFiniteDuration("journal-sequence-retrieval.max-backoff-query-delay", 1.minute),
      askTimeout = config.asFiniteDuration("journal-sequence-retrieval.ask-timeout", 1.second)
    )

}

case class JournalSequenceRetrievalConfig(batchSize: Int,
                                          maxTries: Int,
                                          queryDelay: FiniteDuration,
                                          maxBackoffQueryDelay: FiniteDuration,
                                          askTimeout: FiniteDuration)
