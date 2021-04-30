package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ PartitionKey, PartitionKeyResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.funsuite.AnyFunSuite

import scala.util.control.NonFatal

class PartitionKeyResolverImpl(config: Config) extends PartitionKeyResolver {

  override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
    PartitionKey(persistenceId.asString)
  }

}

class MetricsReporterImpl extends MetricsReporter

object LegacyConfigFormatSpec {

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
}

class LegacyConfigFormatSpec extends AnyFunSuite {
  import LegacyConfigFormatSpec._
  test("load") {
    val journalPluginConfig1 = JournalPluginConfig.fromConfig(
      config(legacyConfigFormat = true, classOf[PartitionKeyResolverImpl].getName, classOf[MetricsReporterImpl].getName)
        .getConfig("j5ik2o.dynamo-db-journal")
    )
    assert(journalPluginConfig1.tableName == "Journal")
    assert(journalPluginConfig1.clientConfig.v2ClientConfig.asyncClientConfig.maxConcurrency == 32)
    assert(journalPluginConfig1.clientConfig.v2ClientConfig.asyncClientConfig.maxPendingConnectionAcquires == 1000)

    val journalPluginConfig2 = JournalPluginConfig.fromConfig(
      config(
        legacyConfigFormat = false,
        classOf[PartitionKeyResolverImpl].getName,
        classOf[MetricsReporterImpl].getName
      )
        .getConfig("j5ik2o.dynamo-db-journal")
    )
    assert(journalPluginConfig2.tableName == "Journal")
    assert(journalPluginConfig2.clientConfig.v2ClientConfig.asyncClientConfig.maxConcurrency == 50)
    assert(journalPluginConfig2.clientConfig.v2ClientConfig.asyncClientConfig.maxPendingConnectionAcquires == 10000)

    try {
      JournalPluginConfig.fromConfig(
        config(legacyConfigFormat = false, "Dummy", classOf[MetricsReporterImpl].getName)
          .getConfig("j5ik2o.dynamo-db-journal")
      )
    } catch {
      case NonFatal(ex: ClassNotFoundException) =>
      case ex =>
        fail(ex)
    }

    try {
      JournalPluginConfig.fromConfig(
        config(legacyConfigFormat = false, "java.lang.String", classOf[MetricsReporterImpl].getName)
          .getConfig("j5ik2o.dynamo-db-journal")
      )
    } catch {
      case NonFatal(ex) =>
        ex.printStackTrace()
    }
  }
}
