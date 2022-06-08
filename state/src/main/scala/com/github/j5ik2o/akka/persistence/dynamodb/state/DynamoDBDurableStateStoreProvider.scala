package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.Done
import akka.actor.{ CoordinatedShutdown, ExtendedActorSystem }
import akka.annotation.ApiMayChange
import akka.event.LoggingAdapter
import akka.persistence.state.DurableStateStoreProvider
import akka.persistence.state.javadsl.{ DurableStateUpdateStore => JavaDurableStateUpdateStore }
import akka.persistence.state.scaladsl.{ DurableStateUpdateStore => ScalaDurableStateUpdateStore }
import akka.stream.{ Materializer, SystemMaterializer }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, MetricsReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.javadsl.JavaDynamoDBDurableStateStore
import com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.{
  DynamoDBDurableStateStoreV1,
  DynamoDBDurableStateStoreV2
}
import com.github.j5ik2o.akka.persistence.dynamodb.trace.{ TraceReporter, TraceReporterProvider }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClientUtils, DispatcherUtils }
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }

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

  private var javaAsyncClientV2: JavaDynamoDbAsyncClient = _
  private var javaSyncClientV2: JavaDynamoDbSyncClient   = _

  CoordinatedShutdown(system).addTask(
    CoordinatedShutdown.PhaseActorSystemTerminate,
    "akka-persistence-dynamodb-state"
  ) { () =>
    Future {
      if (javaAsyncClientV2 != null) javaAsyncClientV2.close()
      if (javaSyncClientV2 != null) javaSyncClientV2.close()
      Done
    }
  }

  private val partitionKeyResolver: PartitionKeyResolver = {
    val provider = PartitionKeyResolverProvider.create(dynamicAccess, statePluginConfig)
    provider.create
  }

  private val tableNameResolver: TableNameResolver = {
    val provider = TableNameResolverProvider.create(dynamicAccess, statePluginConfig)
    provider.create
  }

  def createStore[A]: ScalaDurableStateUpdateStore[A] = statePluginConfig.clientConfig.clientVersion match {
    case ClientVersion.V2 =>
      val (maybeV2SyncClient, maybeV2AsyncClient) = statePluginConfig.clientConfig.clientType match {
        case ClientType.Sync =>
          val client =
            ClientUtils.createV2SyncClient(dynamicAccess, statePluginConfig.configRootPath, statePluginConfig)(
              javaSyncClientV2 = _
            )
          (Some(client), None)
        case ClientType.Async =>
          val client = ClientUtils.createV2AsyncClient(dynamicAccess, statePluginConfig)(javaAsyncClientV2 = _)
          (None, Some(client))
      }
      new DynamoDBDurableStateStoreV2(
        system,
        pluginExecutor,
        maybeV2AsyncClient,
        maybeV2SyncClient,
        partitionKeyResolver,
        tableNameResolver,
        metricsReporter,
        traceReporter,
        statePluginConfig
      )
    case ClientVersion.V1 =>
      val (maybeV1SyncClient, maybeV1AsyncClient) = statePluginConfig.clientConfig.clientType match {
        case ClientType.Sync =>
          val client = ClientUtils
            .createV1SyncClient(dynamicAccess, statePluginConfig.configRootPath, statePluginConfig)
          (Some(client), None)
        case ClientType.Async =>
          val client = ClientUtils.createV1AsyncClient(dynamicAccess, statePluginConfig)
          (None, Some(client))
      }
      new DynamoDBDurableStateStoreV1(
        system,
        pluginExecutor,
        maybeV1AsyncClient,
        maybeV1SyncClient,
        partitionKeyResolver,
        tableNameResolver,
        metricsReporter,
        traceReporter,
        statePluginConfig
      )
  }

  override def scaladslDurableStateStore(): ScalaDurableStateUpdateStore[Any] = createStore[Any]

  override def javadslDurableStateStore(): JavaDurableStateUpdateStore[AnyRef] =
    new JavaDynamoDBDurableStateStore[AnyRef](system, createStore[AnyRef])
}
