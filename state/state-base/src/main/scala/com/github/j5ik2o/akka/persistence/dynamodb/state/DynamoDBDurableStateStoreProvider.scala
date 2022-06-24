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
package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.actor.ExtendedActorSystem
import akka.annotation.ApiMayChange
import akka.event.LoggingAdapter
import akka.persistence.state.DurableStateStoreProvider
import akka.persistence.state.javadsl.{ DurableStateUpdateStore => JavaDurableStateUpdateStore }
import akka.persistence.state.scaladsl.{ DurableStateUpdateStore => ScalaDurableStateUpdateStore }
import akka.stream.{ Materializer, SystemMaterializer }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.javadsl.JavaDynamoDBDurableStateStore
import com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.ScalaDurableStateUpdateStoreFactory
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessUtils
import com.typesafe.config.Config

import java.util.UUID
import scala.concurrent.ExecutionContext

object DynamoDBDurableStateStoreProvider {

  val Identifier = "j5ik2o.dynamo-db-state"

}

@ApiMayChange
final class DynamoDBDurableStateStoreProvider(system: ExtendedActorSystem) extends DurableStateStoreProvider {

  implicit val mat: Materializer    = SystemMaterializer(system).materializer
  implicit val _log: LoggingAdapter = system.log

  private val id: UUID = UUID.randomUUID()
  _log.debug("dynamodb state store provider: id = {}", id)

  system.dynamicAccess

  private val config: Config = system.settings.config.getConfig(DynamoDBDurableStateStoreProvider.Identifier)
  private val statePluginConfig: StatePluginConfig = StatePluginConfig.fromConfig(config)
  private val statePluginContext                   = StatePluginContext(system, statePluginConfig)

  import statePluginContext._

  implicit val ec: ExecutionContext = pluginExecutor

  private def createStore[A]
      : com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.ScalaDurableStateUpdateStore[A] = {
    val className = statePluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V2 =>
        "com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.V2ScalaDurableStateUpdateStoreFactory"
      case ClientVersion.V2Dax =>
        "com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.V2DaxScalaDurableStateUpdateStoreFactory"
      case ClientVersion.V1 =>
        "com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.V1ScalaDurableStateUpdateStoreFactory"
      case ClientVersion.V1Dax =>
        "com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.V1DaxScalaDurableStateUpdateStoreFactory"
    }
    val f =
      DynamicAccessUtils.createInstanceFor_CTX_Throw[ScalaDurableStateUpdateStoreFactory, StatePluginContext](
        className,
        statePluginContext,
        classOf[StatePluginContext]
      )
    f.create
  }

  override def scaladslDurableStateStore(): ScalaDurableStateUpdateStore[Any] = createStore[Any]

  override def javadslDurableStateStore(): JavaDurableStateUpdateStore[AnyRef] = {
    val store = createStore[AnyRef]
    new JavaDynamoDBDurableStateStore[AnyRef](system, pluginExecutor, store)
  }
}
