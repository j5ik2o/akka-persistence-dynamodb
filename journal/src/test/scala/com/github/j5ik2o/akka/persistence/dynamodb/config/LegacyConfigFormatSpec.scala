package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.journal.{
  PartitionKey,
  PartitionKeyResolver,
  PersistenceId,
  SequenceNumber
}
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ FreeSpec, Matchers }

import scala.concurrent.duration.Duration

class PartitionKeyResolverImpl(config: Config) extends PartitionKeyResolver {

  override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
    PartitionKey(persistenceId.asString)
  }

}

class MetricsReporterImpl extends MetricsReporter {
  override def setDynamoDBClientPutItemDuration(duration: Duration): Unit        = {}
  override def setDynamoDBClientBatchWriteItemDuration(duration: Duration): Unit = {}
  override def setDynamoDBClientUpdateItemDuration(duration: Duration): Unit     = {}
  override def setDynamoDBClientDeleteItemDuration(duration: Duration): Unit     = {}
  override def setDynamoDBClientQueryDuration(duration: Duration): Unit          = {}
  override def setDynamoDBClientScanDuration(duration: Duration): Unit           = {}
}

class LegacyConfigFormatSpec extends FreeSpec with Matchers {
  "config" - {
    "load" in {
      def config(legacyConfigFormat: Boolean, partitionKeyResolverClassName: String, metricsReporterClassName: String) =
        ConfigFactory.parseString(
          s"""
          |j5ik2o {
          |  dynamo-db-journal {
          |    legacy-config-format = ${legacyConfigFormat}
          |    table-name = "Journal"
          |    get-journal-rows-index-name = "GetJournalRowsIndex"
          |    tags-index-name = "TagsIndex"
          |    shard-count = 32
          |    partition-key-resolver-class-name = "$partitionKeyResolverClassName"
          |    queue-enable = false
          |    queue-buffer-size = 32
          |    queue-overflow-strategy = "Fail"
          |    queue-parallelism = 32
          |    write-parallelism = 32
          |    query-batch-size = 32
          |    scan-batch-size = 512
          |    replay-batch-size = 32
          |    consistent-read = false
          |    soft-delete = true
          |    metrics-reporter-class-name = "$metricsReporterClassName"
          |    dynamo-db-client {
          |      max-concurrency = 32
          |      max-pending-connection-acquires = 1000
          |      read-timeout = 10s
          |      write-timeout = 10s
          |      connection-timeout = 10s
          |      connection-acquisition-timeout = 10s
          |      connection-time-to-live = 10s
          |      max-idle-connection-timeout = 10s
          |      use-connection-reaper = true
          |      threads-of-event-loop-group = 32
          |      use-http2 = false
          |      http2-max-streams = 32
          |      batch-get-item-limit = 100
          |      batch-write-item-limit = 25
          |    }
          |  }
          |
          |}
          |""".stripMargin
        )
      val journalPluginConfig1 = JournalPluginConfig.fromConfig(
        config(true, classOf[PartitionKeyResolverImpl].getName, classOf[MetricsReporterImpl].getName)
          .getConfig("j5ik2o.dynamo-db-journal")
      )
      journalPluginConfig1.tableName shouldBe "Journal"
      journalPluginConfig1.clientConfig.v2ClientConfig.asyncClientConfig.maxConcurrency shouldBe 32
      journalPluginConfig1.clientConfig.v2ClientConfig.asyncClientConfig.maxPendingConnectionAcquires shouldBe 1000

      val journalPluginConfig2 = JournalPluginConfig.fromConfig(
        config(false, classOf[PartitionKeyResolverImpl].getName, classOf[MetricsReporterImpl].getName)
          .getConfig("j5ik2o.dynamo-db-journal")
      )
      journalPluginConfig2.tableName shouldBe "Journal"
      journalPluginConfig2.clientConfig.v2ClientConfig.asyncClientConfig.maxConcurrency shouldBe 50
      journalPluginConfig2.clientConfig.v2ClientConfig.asyncClientConfig.maxPendingConnectionAcquires shouldBe 10000

      an[ClassNotFoundException] should be thrownBy {
        JournalPluginConfig.fromConfig(
          config(false, "Dummy", classOf[MetricsReporterImpl].getName)
            .getConfig("j5ik2o.dynamo-db-journal")
        )
      }
      val ex = the[IllegalArgumentException] thrownBy {
        JournalPluginConfig.fromConfig(
          config(false, "java.lang.String", classOf[MetricsReporterImpl].getName)
            .getConfig("j5ik2o.dynamo-db-journal")
        )
      }
      ex.printStackTrace()
    }
  }
}
