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
