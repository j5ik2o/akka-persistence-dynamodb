/*
 * Copyright 2019 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config

object DynamoDBClientConfig extends LoggingSupport {

  def fromConfig(config: Config, legacy: Boolean): DynamoDBClientConfig = {
    val result = DynamoDBClientConfig(
      accessKeyId = config.asString("access-key-id"),
      secretAccessKey = config.asString("secret-access-key"),
      endpoint = config.asString("endpoint"),
      region = config.asString("region"),
      clientVersion = config.asString("client-version").map(s => ClientVersion.withName(s)).getOrElse(ClientVersion.V2),
      clientType = config.asString("client-type").map(s => ClientType.withName(s)).getOrElse(ClientType.Async),
      DynamoDBClientV1Config.from(config.asConfig("v1")),
      DynamoDBClientV1DaxConfig.from(config.asConfig("v1-dax")), {
        if (legacy) {
          logger.warn("[[Please migration to the new config format.]]")
          DynamoDBClientV2Config.from(config, legacy)
        } else
          DynamoDBClientV2Config.from(config.asConfig("v2"), legacy)
      },
      batchGetItemLimit = config.asInt("batch-get-item-limit", 100),
      batchWriteItemLimit = config.asInt("batch-write-item-limit", 25)
    )
    logger.debug("config = {}", result)
    result
  }

}

object ClientVersion extends Enumeration {
  val V1: ClientVersion.Value    = Value("v1")
  val V1Dax: ClientVersion.Value = Value("v1-dax")
  val V2: ClientVersion.Value    = Value("v2")
}

object ClientType extends Enumeration {
  val Sync: ClientType.Value  = Value("sync")
  val Async: ClientType.Value = Value("async")
}

case class DynamoDBClientConfig(
    accessKeyId: Option[String],
    secretAccessKey: Option[String],
    endpoint: Option[String],
    region: Option[String],
    clientVersion: ClientVersion.Value,
    clientType: ClientType.Value,
    v1ClientConfig: DynamoDBClientV1Config,
    v1DaxClientConfig: DynamoDBClientV1DaxConfig,
    v2ClientConfig: DynamoDBClientV2Config,
    batchGetItemLimit: Int, // Currently unused
    batchWriteItemLimit: Int
) {
  require(batchGetItemLimit >= 1 && batchGetItemLimit <= 100)
  require(batchWriteItemLimit >= 1 && batchWriteItemLimit <= 25)
}
