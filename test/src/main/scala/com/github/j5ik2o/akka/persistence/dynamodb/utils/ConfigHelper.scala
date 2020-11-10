package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.typesafe.config.{ Config, ConfigFactory }

object ConfigHelper {

  def config(
      defaultResource: Option[String],
      legacyConfigFormat: Boolean,
      legacyJournalMode: Boolean,
      dynamoDBPort: Int,
      clientVersion: String,
      clientType: String,
      requestHandlerClassNames: Seq[String] = Seq.empty,
      journalRowDriverWrapperClassName: Option[String] = None
  ): Config = {
    val configString = s"""
       |akka.persistence.journal.plugin = "j5ik2o.dynamo-db-journal"
       |akka.persistence.snapshot-store.plugin = "j5ik2o.dynamo-db-snapshot"
       |j5ik2o.dynamo-db-journal {
       |  legacy-config-format = $legacyConfigFormat
       |  shard-count = 1024
       |  queue-enable = true
       |  queue-overflow-strategy = backpressure 
       |  queue-buffer-size = 1024
       |  queue-parallelism = 32
       |  write-parallelism = 1
       |  query-batch-size = 1024
       |  dynamo-db-client {
       |    region = "ap-northeast-1"
       |    access-key-id = "x"
       |    secret-access-key = "x" 
       |    endpoint = "http://127.0.0.1:${dynamoDBPort}/"
       |    client-version = "${clientVersion.toLowerCase}"
       |    client-type = "${clientType.toLowerCase()}"
       |    v2 {
       |      dispatcher-name = "journal-blocking-io-dispatcher"
       |      async {
       |        max-concurrency = 64  
       |      }
       |    }
       |    v1 {
       |    ${if (requestHandlerClassNames.nonEmpty) {
                            s"""request-handler-class-names = ["${requestHandlerClassNames.mkString(",")}"]"""
                          } else ""}
       |      dispatcher-name = "journal-blocking-io-dispatcher"
       |      async {
       |      }
       |    } 
       |  }
       |  ${if (journalRowDriverWrapperClassName.nonEmpty) {
                            s"""journal-row-driver-wrapper-class-name = "${journalRowDriverWrapperClassName.get}" """
                          } else ""}
       |  columns-def {
       |    sort-key-column-name = ${if (legacyJournalMode) "sequence-nr" else "skey"}
       |  }
       |}
       |
       |j5ik2o.dynamo-db-snapshot {
       |  dynamo-db-client {
       |    region = "ap-northeast-1"
       |    access-key-id = "x"
       |    secret-access-key = "x" 
       |    endpoint = "http://127.0.0.1:${dynamoDBPort}/"
       |    v2 {
       |      dispatcher-name = "snapshot-blocking-io-dispatcher" 
       |    }
       |    v1 {
       |      dispatcher-name = "snapshot-blocking-io-dispatcher" 
       |    }
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
       |journal-blocking-io-dispatcher {
       |  type = "Dispatcher"
       |  executor = "thread-pool-executor"
       |  thread-pool-executor {
       |    fixed-pool-size = 64
       |  }
       |}
       |snapshot-blocking-io-dispatcher {
       |  type = "Dispatcher"
       |  executor = "thread-pool-executor"
       |  thread-pool-executor {
       |    fixed-pool-size = 64 
       |  }
       |}
       """.stripMargin
    val config = ConfigFactory
      .parseString(
        configString
      ).withFallback(
        defaultResource.fold(ConfigFactory.load())(ConfigFactory.load)
      )
    // println(ConfigRenderUtils.renderConfigToString(config))
    config
  }
}
