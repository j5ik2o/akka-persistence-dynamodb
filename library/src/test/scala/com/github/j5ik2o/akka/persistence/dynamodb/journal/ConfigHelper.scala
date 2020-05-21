package com.github.j5ik2o.akka.persistence.dynamodb.journal

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.typesafe.config.{ Config, ConfigFactory }

object ConfigHelper {

  def config(
      legacyConfigFormat: Boolean,
      legacyJournalMode: Boolean,
      dynamoDBPort: Int,
      clientVersion: ClientVersion.Value,
      clientType: ClientType.Value
  ): Config = {
    ConfigFactory
      .parseString(
        s"""
           |j5ik2o.dynamo-db-journal {
           |  legacy-config-format = ${legacyConfigFormat}
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
           |  columns-def {
           |    sort-key-column-name = ${if (legacyJournalMode) "sequence-nr" else "skey"}
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
           """.stripMargin
      ).withFallback(ConfigFactory.load())
  }
}
