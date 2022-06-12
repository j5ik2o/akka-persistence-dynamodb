package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl
import akka.actor.{ ActorSystem, DynamicAccess }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ PartitionKeyResolver, TableNameResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V1AsyncClientFactory, V1SyncClientFactory }

import scala.collection.immutable
import scala.concurrent.ExecutionContext

class V1ScalaDurableStateUpdateStoreFactory extends ScalaDurableStateUpdateStoreFactory {
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
    val (maybeV1SyncClient, maybeV1AsyncClient) = pluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val f = dynamicAccess
          .createInstanceFor[V1SyncClientFactory](pluginConfig.v1SyncClientFactoryClassName, immutable.Seq.empty).get
        val client = f.create(dynamicAccess, pluginConfig)
        (Some(client), None)
      case ClientType.Async =>
        val f = dynamicAccess
          .createInstanceFor[V1AsyncClientFactory](pluginConfig.v1AsyncClientFactoryClassName, immutable.Seq.empty).get
        val client = f.create(dynamicAccess, pluginConfig)
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
