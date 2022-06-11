package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.actor.{ ActorSystem, DynamicAccess }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ PartitionKeyResolver, TableNameResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.V1ClientUtils

import scala.concurrent.ExecutionContext

class V1DaxScalaDurableStateUpdateStoreFactory extends ScalaDurableStateUpdateStoreFactory {
  override def create[A](
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      pluginExecutor: ExecutionContext,
      partitionKeyResolver: PartitionKeyResolver,
      tableNameResolver: TableNameResolver,
      metricsReporter: Option[MetricsReporter],
      traceReporter: Option[TraceReporter],
      pluginConfig: StatePluginConfig
  ): ScalaDurableStateUpdateStore[A] = {
    implicit val log = system.log
    val (maybeV1SyncClient, maybeV1AsyncClient) = pluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val client = V1ClientUtils
          .createV1DaxSyncClient(pluginConfig.configRootPath, pluginConfig.clientConfig)
        (Some(client), None)
      case ClientType.Async =>
        val client = V1ClientUtils.createV1DaxAsyncClient(pluginConfig.clientConfig)
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
      pluginConfig
    )
  }
}
