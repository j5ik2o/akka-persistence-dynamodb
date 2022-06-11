package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao
import akka.actor.{ ActorSystem, DynamicAccess }
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.V2ClientUtils

class V2SnapshotDaoFactory extends SnapshotDaoFactory {
  override def create(
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      serialization: Serialization,
      pluginConfig: SnapshotPluginConfig,
      metricsReporter: Option[MetricsReporter],
      traceReporter: Option[TraceReporter]
  ): SnapshotDao = {
    implicit val log = system.log
    val (async, sync) = pluginConfig.clientConfig.clientType match {
      case ClientType.Async =>
        val v1JavaAsyncClient = V2ClientUtils.createV2AsyncClient(dynamicAccess, pluginConfig)
        (Some(v1JavaAsyncClient), None)
      case ClientType.Sync =>
        val v1JavaSyncClient =
          V2ClientUtils.createV2SyncClient(dynamicAccess, pluginConfig.configRootPath, pluginConfig)
        (None, Some(v1JavaSyncClient))
    }
    new V2SnapshotDaoImpl(system, async, sync, serialization, pluginConfig, metricsReporter, traceReporter)
  }
}
