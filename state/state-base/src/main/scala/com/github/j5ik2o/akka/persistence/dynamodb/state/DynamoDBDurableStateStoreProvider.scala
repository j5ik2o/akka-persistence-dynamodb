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
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.javadsl.JavaDynamoDBDurableStateStore
import com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.ScalaDurableStateUpdateStoreFactory
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.typesafe.config.Config

import java.util.UUID
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

object DynamoDBDurableStateStoreProvider {

  val Identifier = "j5ik2o.dynamo-db-state"

}

@ApiMayChange
final class DynamoDBDurableStateStoreProvider(system: ExtendedActorSystem) extends DurableStateStoreProvider {

  implicit val mat: Materializer    = SystemMaterializer(system).materializer
  implicit val _log: LoggingAdapter = system.log

  private val id: UUID = UUID.randomUUID()
  _log.debug("dynamodb state store provider: id = {}", id)

  private val dynamicAccess = system.dynamicAccess

  private val config: Config = system.settings.config.getConfig(DynamoDBDurableStateStoreProvider.Identifier)
  private val statePluginConfig: StatePluginConfig = StatePluginConfig.fromConfig(config)

  private val pluginExecutor: ExecutionContext =
    statePluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V1 =>
        DispatcherUtils.newV1Executor(statePluginConfig, system)
      case ClientVersion.V2 =>
        DispatcherUtils.newV2Executor(statePluginConfig, system)
    }

  implicit val ec: ExecutionContext = pluginExecutor

  protected val metricsReporter: Option[MetricsReporter] = {
    val metricsReporterProvider = MetricsReporterProvider.create(dynamicAccess, statePluginConfig)
    metricsReporterProvider.create
  }

  protected val traceReporter: Option[TraceReporter] = {
    val traceReporterProvider = TraceReporterProvider.create(dynamicAccess, statePluginConfig)
    traceReporterProvider.create
  }

  private val partitionKeyResolver: PartitionKeyResolver = {
    val provider = PartitionKeyResolverProvider.create(dynamicAccess, statePluginConfig)
    provider.create
  }

  private val tableNameResolver: TableNameResolver = {
    val provider = TableNameResolverProvider.create(dynamicAccess, statePluginConfig)
    provider.create
  }

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
    val f = dynamicAccess.createInstanceFor[ScalaDurableStateUpdateStoreFactory](className, immutable.Seq.empty) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize ScalaDurableStateUpdateStoreFactory", Some(ex))
    }
    f.create(
      system,
      dynamicAccess,
      pluginExecutor,
      partitionKeyResolver,
      tableNameResolver,
      metricsReporter,
      traceReporter,
      statePluginConfig
    )
  }

  override def scaladslDurableStateStore(): ScalaDurableStateUpdateStore[Any] = createStore[Any]

  override def javadslDurableStateStore(): JavaDurableStateUpdateStore[AnyRef] = {
    val store = createStore[AnyRef]
    new JavaDynamoDBDurableStateStore[AnyRef](system, pluginExecutor, store)
  }
}
