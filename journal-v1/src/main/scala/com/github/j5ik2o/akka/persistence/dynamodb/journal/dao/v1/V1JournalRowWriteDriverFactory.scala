package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import akka.actor.{ ActorSystem, DynamicAccess }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientType
import com.github.j5ik2o.akka.persistence.dynamodb.journal.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{
  JournalRowWriteDriverFactory,
  PartitionKeyResolver,
  SortKeyResolver
}
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V1AsyncClientFactory, V1SyncClientFactory }

import scala.collection.immutable

class V1JournalRowWriteDriverFactory extends JournalRowWriteDriverFactory {

  override def create(
      system: ActorSystem,
      dynamicAccess: DynamicAccess,
      journalPluginConfig: JournalPluginConfig,
      partitionKeyResolver: PartitionKeyResolver,
      sortKeyResolver: SortKeyResolver,
      metricsReporter: Option[MetricsReporter]
  ): JournalRowWriteDriver = {
    val (maybeSyncClient, maybeAsyncClient) = journalPluginConfig.clientConfig.clientType match {
      case ClientType.Sync =>
        val f = dynamicAccess
          .createInstanceFor[V1SyncClientFactory](
            journalPluginConfig.v1SyncClientFactoryClassName,
            immutable.Seq.empty
          ).get
        val client = f.create(dynamicAccess, journalPluginConfig)
        (Some(client), None)
      case ClientType.Async =>
        val f = dynamicAccess
          .createInstanceFor[V1AsyncClientFactory](
            journalPluginConfig.v1AsyncClientFactoryClassName,
            immutable.Seq.empty
          ).get
        val client = f.create(dynamicAccess, journalPluginConfig)
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
