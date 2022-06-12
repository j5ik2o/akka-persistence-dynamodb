/*
 * Copyright 2022 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.client

import akka.NotUsed
import akka.stream.RestartSettings
import akka.stream.scaladsl.{ Flow, RestartFlow }
import com.github.j5ik2o.akka.persistence.dynamodb.config.BackoffConfig

trait StreamSupport {

  def flowWithBackoffSettings[In, Out](
      backoffConfig: BackoffConfig,
      flow: Flow[In, Out, NotUsed]
  ): Flow[In, Out, NotUsed] = {
    if (backoffConfig.enabled) {
      val restartSettings = RestartSettings(
        minBackoff = backoffConfig.minBackoff,
        maxBackoff = backoffConfig.maxBackoff,
        randomFactor = backoffConfig.randomFactor
      ).withMaxRestarts(backoffConfig.maxRestarts, backoffConfig.minBackoff)
      RestartFlow
        .withBackoff(
          restartSettings
        ) { () => flow }
    } else flow
  }
}
