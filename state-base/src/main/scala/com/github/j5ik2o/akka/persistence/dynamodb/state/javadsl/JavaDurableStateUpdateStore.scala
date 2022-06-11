package com.github.j5ik2o.akka.persistence.dynamodb.state.javadsl

import akka.persistence.state.javadsl.DurableStateUpdateStore
import com.github.j5ik2o.akka.persistence.dynamodb.state.GetRawObjectResult

import java.util.concurrent.CompletionStage

trait JavaDurableStateUpdateStore[A] extends DurableStateUpdateStore[A] {
  def getRawObject(persistenceId: String): CompletionStage[GetRawObjectResult[A]]
}
