package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao
import akka.actor.{ ActorSystem, DynamicAccess }
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V2AsyncClientFactory, V2SyncClientFactory }

import scala.collection.immutable

class V2SnapshotDaoFactory extends SnapshotDaoFactory {
  override def create(
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      serialization: Serialization,
      pluginConfig: SnapshotPluginConfig,
      metricsReporter: Option[MetricsReporter],
      traceReporter: Option[TraceReporter]
  ): SnapshotDao = {
    val (async, sync) = pluginConfig.clientConfig.clientType match {
      case ClientType.Async =>
        val f = dynamicAccess
          .createInstanceFor[V2AsyncClientFactory](pluginConfig.v2AsyncClientFactoryClassName, immutable.Seq.empty).get
        val v1JavaAsyncClient = f.create(dynamicAccess, pluginConfig)
        (Some(v1JavaAsyncClient), None)
      case ClientType.Sync =>
        val f = dynamicAccess
          .createInstanceFor[V2SyncClientFactory](pluginConfig.v2SyncClientFactoryClassName, immutable.Seq.empty).get
        val v1JavaSyncClient = f.create(dynamicAccess, pluginConfig)
        (None, Some(v1JavaSyncClient))
    }
    new V2SnapshotDaoImpl(system, async, sync, serialization, pluginConfig, metricsReporter, traceReporter)
  }
}
