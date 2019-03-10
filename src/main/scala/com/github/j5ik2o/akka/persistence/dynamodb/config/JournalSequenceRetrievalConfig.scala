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
