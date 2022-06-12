package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

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
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ V2AsyncClientFactory, V2SyncClientFactory }

import scala.collection.immutable

class V2JournalRowWriteDriverFactory extends JournalRowWriteDriverFactory {

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
          .createInstanceFor[V2SyncClientFactory](
            journalPluginConfig.v2SyncClientFactoryClassName,
            immutable.Seq.empty
          ).get
        val client = f.create(dynamicAccess, journalPluginConfig)
        (Some(client), None)
      case ClientType.Async =>
        val f = dynamicAccess
          .createInstanceFor[V2AsyncClientFactory](
            journalPluginConfig.v2AsyncClientFactoryClassName,
            immutable.Seq.empty
          ).get
        val client = f.create(dynamicAccess, journalPluginConfig)
        (None, Some(client))
    }
    new V2JournalRowWriteDriver(
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
