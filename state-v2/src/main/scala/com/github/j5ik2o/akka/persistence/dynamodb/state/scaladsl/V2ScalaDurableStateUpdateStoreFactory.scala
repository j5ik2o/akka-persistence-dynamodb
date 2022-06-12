package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.actor.{ ActorSystem, DynamicAccess }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ PartitionKeyResolver, TableNameResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V2AsyncClientFactory, V2SyncClientFactory }

import scala.collection.immutable
import scala.concurrent.ExecutionContext

class V2ScalaDurableStateUpdateStoreFactory extends ScalaDurableStateUpdateStoreFactory {
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
    val (maybeV2SyncClient, maybeV2AsyncClient) = pluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val f = dynamicAccess
          .createInstanceFor[V2SyncClientFactory](pluginConfig.v2SyncClientFactoryClassName, immutable.Seq.empty).get
        val client = f.create(dynamicAccess, pluginConfig)
        (Some(client), None)
      case ClientType.Async =>
        val f = dynamicAccess
          .createInstanceFor[V2AsyncClientFactory](pluginConfig.v2AsyncClientFactoryClassName, immutable.Seq.empty).get
        val client = f.create(dynamicAccess, pluginConfig)
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
      pluginConfig
    )
  }
}
