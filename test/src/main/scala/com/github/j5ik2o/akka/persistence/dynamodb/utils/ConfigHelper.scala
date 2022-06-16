/*
 * Copyright 2020 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.typesafe.config.{ Config, ConfigFactory }

object ConfigHelper {

  def config(
      defaultResource: Option[String],
      legacyConfigFormat: Boolean,
      legacyJournalMode: Boolean,
      dynamoDBHost: String,
      dynamoDBPort: Int,
      clientVersion: String,
      clientType: String,
      requestHandlerClassNames: Seq[String] = Seq.empty,
      journalRowDriverWrapperClassName: Option[String] = None,
      softDelete: Boolean = true
  ): Config = {
    val configString = s"""
       |akka.persistence.journal.plugin = "j5ik2o.dynamo-db-journal"
       |akka.persistence.snapshot-store.plugin = "j5ik2o.dynamo-db-snapshot"
       |akka.persistence.state.plugin = "j5ik2o.dynamo-db-state"
       |j5ik2o.dynamo-db-journal {
       |  legacy-config-format = $legacyConfigFormat
       |  shard-count = 1024
       |  queue-enable = true
       |  queue-overflow-strategy = backpressure
       |  queue-buffer-size = 1024
       |  queue-parallelism = 32
       |  write-parallelism = 1
       |  query-batch-size = 1024
       |  soft-delete = $softDelete
       |  dynamo-db-client {
       |    region = "ap-northeast-1"
       |    access-key-id = "x"
       |    secret-access-key = "x"
       |    endpoint = "http://$dynamoDBHost:$dynamoDBPort/"
       |    client-version = "${clientVersion.toLowerCase}"
       |    client-type = "${clientType.toLowerCase()}"
       |    v2 {
       |      async {
       |        max-concurrency = 64
       |      }
       |      sync {
       |        dispatcher-name = "journal-blocking-io-dispatcher"
       |        max-connections = 64
       |      }
       |    }
       |    v1 {
       |    ${if (requestHandlerClassNames.nonEmpty) {
                           s"""request-handler-class-names = ["${requestHandlerClassNames.mkString(",")}"]"""
                         } else ""}
       |      dispatcher-name = "journal-blocking-io-dispatcher"
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
       |    endpoint = "http://$dynamoDBHost:$dynamoDBPort/"
       |    v2 {
       |      dispatcher-name = "snapshot-blocking-io-dispatcher"
       |    }
       |    v1 {
       |      dispatcher-name = "snapshot-blocking-io-dispatcher"
       |    }
       |  }
       |}
       |
       |j5ik2o.dynamo-db-state {
       |  dynamo-db-client {
       |    region = "ap-northeast-1"
       |    access-key-id = "x"
       |    secret-access-key = "x"
       |    endpoint = "http://$dynamoDBHost:$dynamoDBPort/"
       |    client-version = "${clientVersion.toLowerCase}"
       |    client-type = "${clientType.toLowerCase()}"
       |    v2 {
       |      dispatcher-name = "state-blocking-io-dispatcher"
       |    }
       |    v1 {
       |      dispatcher-name = "state-blocking-io-dispatcher"
       |    }
       |  }
       |}
       |
       |j5ik2o.dynamo-db-read-journal {
       |  query-batch-size = 1
       |  dynamo-db-client {
       |    region = "ap-northeast-1"
       |    endpoint = "http://$dynamoDBHost:$dynamoDBPort/"
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
       |
       |snapshot-blocking-io-dispatcher {
       |  type = "Dispatcher"
       |  executor = "thread-pool-executor"
       |  thread-pool-executor {
       |    fixed-pool-size = 64
       |  }
       |}
       |
       |state-blocking-io-dispatcher {
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
