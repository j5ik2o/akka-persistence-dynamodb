package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import akka.actor.{ ActorSystem, DynamicAccess }
import akka.event.LoggingAdapter
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.journal.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{
  JournalRowWriteDriverFactory,
  PartitionKeyResolver,
  SortKeyResolver
}
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.V1ClientUtils

class V1JournalRowWriteDriverFactory extends JournalRowWriteDriverFactory {

  override def create(
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      journalPluginConfig: JournalPluginConfig,
      partitionKeyResolver: PartitionKeyResolver,
      sortKeyResolver: SortKeyResolver,
      metricsReporter: Option[MetricsReporter]
  ): JournalRowWriteDriver = {
    implicit val log: LoggingAdapter = system.log
    val (maybeSyncClient, maybeAsyncClient) = journalPluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val client = V1ClientUtils
          .createV1SyncClient(dynamicAccess, journalPluginConfig.configRootPath, journalPluginConfig)
        (Some(client), None)
      case ClientType.Async =>
        val client = V1ClientUtils.createV1AsyncClient(dynamicAccess, journalPluginConfig)
        (None, Some(client))
    }
    new V1JournalRowWriteDriver(
      system,
      maybeAsyncClient,
      maybeSyncClient,
      journalPluginConfig,
      partitionKeyResolver,
      sortKeyResolver,
      metricsReporter
    )
  }
}
