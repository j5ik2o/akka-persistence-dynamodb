package com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigRenderUtils
import com.typesafe.config.{ Config, ConfigFactory }

object ConfigHelper {

  def config(
      defaultResource: String,
      legacyConfigFormat: Boolean,
      legacyJournalMode: Boolean,
      dynamoDBPort: Int,
      clientVersion: ClientVersion.Value,
      clientType: ClientType.Value,
      journalRowDriverWrapperClassName: Option[String],
      kafkaPort: Option[Int]
  ): Config = {
    val configString = s"""
       |akka.persistence.journal.plugin = "j5ik2o.dynamo-db-journal"
       |akka.persistence.snapshot-store.plugin = "j5ik2o.dynamo-db-snapshot"
       |j5ik2o.dynamo-db-journal {
       |  legacy-config-format = $legacyConfigFormat
       |  shard-count = 1024
       |  queue-enable = true
       |  queue-overflow-strategy = fail
       |  queue-buffer-size = 1024
       |  queue-parallelism = 1
       |  write-parallelism = 1
       |  query-batch-size = 1024
       |  dynamo-db-client {
       |    region = "ap-northeast-1"
       |    endpoint = "http://127.0.0.1:${dynamoDBPort}/"
       |    client-version = "${clientVersion.toString.toLowerCase}"
       |    client-type = "${clientType.toString.toLowerCase()}"
       |  }
       |  ${if (journalRowDriverWrapperClassName.nonEmpty) {
                            s"""journal-row-driver-wrapper-class-name = "${journalRowDriverWrapperClassName.get}" """
                          } else ""}
       |  columns-def {
       |    sort-key-column-name = ${if (legacyJournalMode) "sequence-nr" else "skey"}
       |  }
       |  kafka-write-adaptor {
       |    pass-through = false
       |    partition-size = 128
       |    producer {
       |      discovery-method = akka.discovery
       |      service-name = ""
       |      resolve-timeout = 3 seconds
       |      parallelism = 100
       |      close-timeout = 60s
       |      close-on-producer-stop = true
       |      use-dispatcher = "producer-dispatcher"
       |      eos-commit-interval = 100ms
       |      kafka-clients {
           ${if (kafkaPort.nonEmpty) {
                            s"""bootstrap.servers = "http://127.0.0.1:${kafkaPort.get}" """
                          } else ""}
       |      }
       |    }
       |  }
       |}
       |
       |j5ik2o.dynamo-db-snapshot {
       |  dynamo-db-client {
       |    region = "ap-northeast-1"
       |    endpoint = "http://127.0.0.1:${dynamoDBPort}/"
       |  }
       |}
       |
       |j5ik2o.dynamo-db-read-journal {
       |  query-batch-size = 1
       |  dynamo-db-client {
       |    region = "ap-northeast-1"
       |    endpoint = "http://127.0.0.1:${dynamoDBPort}/"
       |  }
       |  columns-def {
       |    sort-key-column-name = ${if (legacyJournalMode) "sequence-nr" else "skey"}
       |  }
       |}
       |
       |j5ik2o.dynamo-db-journal.kafka-to-ddb-projector {
       |  parallelism = 8
       |  consumer {
       |    discovery-method = akka.discovery
       |    service-name = ""
       |    resolve-timeout = 3 seconds
       |    poll-interval = 50ms
       |    poll-timeout = 50ms
       |    stop-timeout = 30s
       |    close-timeout = 20s
       |    commit-timeout = 15s
       |    commit-time-warning = 1s
       |    commit-refresh-interval = infinite
       |    use-dispatcher = "consumer-dispatcher"
       |    kafka-clients {
       |      enable.auto.commit = false
       |      auto.offset.reset = earliest 
       |      group.id = "test"
       |${if (kafkaPort.nonEmpty) {
                            s"""bootstrap.servers = "http://127.0.0.1:${kafkaPort.get}" """
                          } else ""}
       |    }
       |    wait-close-partition = 500ms
       |    position-timeout = 5s
       |    offset-for-times-timeout = 5s
       |    metadata-request-timeout = 5s
       |    eos-draining-check-interval = 30ms
       |    partition-handler-warning = 5s
       |    connection-checker {
       |      enable = false
       |      max-retries = 3
       |      check-interval = 15s
       |      backoff-factor = 2.0
       |    }
       |  }
       |  committer-settings {
       |    max-batch = 1000
       |    max-interval = 10s
       |    parallelism = 100
       |    delivery = WaitForAck
       |  }
       |}
           """.stripMargin
    val config = ConfigFactory
      .parseString(
        configString
      ).withFallback(ConfigFactory.load(defaultResource))
    println(ConfigRenderUtils.renderConfigToString(config))
    config
  }
}
