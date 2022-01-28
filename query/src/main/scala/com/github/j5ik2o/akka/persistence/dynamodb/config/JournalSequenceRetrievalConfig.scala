/*
 * Copyright 2019 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.config.ConfigSupport._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config

import scala.concurrent.duration._

object JournalSequenceRetrievalConfig extends LoggingSupport {

  val batchSizeKey  = "batch-size"
  val maxTriesKey   = "max-tries"
  val queryDelayKey = "query-delay"

  def fromConfig(config: Config): JournalSequenceRetrievalConfig = {
    logger.debug("config = {}", config)
    val result = JournalSequenceRetrievalConfig(
      sourceConfig = config,
      batchSize = config.valueAs(batchSizeKey, 10000),
      maxTries = config.valueAs(maxTriesKey, 10),
      queryDelay = config.valueAs[FiniteDuration](queryDelayKey, 1.second),
      maxBackoffQueryDelay = config.valueAs[FiniteDuration]("max-backoff-query-delay", 1.minute),
      askTimeout = config.valueAs[FiniteDuration]("ask-timeout", 1.second)
    )
    logger.debug("result = {}", result)
    result
  }

}

case class JournalSequenceRetrievalConfig(
    sourceConfig: Config,
    batchSize: Int,
    maxTries: Int,
    queryDelay: FiniteDuration,
    maxBackoffQueryDelay: FiniteDuration,
    askTimeout: FiniteDuration
)
