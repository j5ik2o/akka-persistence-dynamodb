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
package com.github.j5ik2o.akka.persistence.dynamodb.config.client

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1.DynamoDBClientV1Config
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1dax.DynamoDBClientV1DaxConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2.DynamoDBClientV2Config
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._

object DynamoDBClientConfig extends LoggingSupport {

  val accessKeyIdKeyKey      = "access-key-id"
  val secretAccessKeyKey     = "secret-access-key"
  val endpointKey            = "endpoint"
  val regionKey              = "region"
  val clientVersionKey       = "client-version"
  val clientTypeKey          = "client-type"
  val v1Key                  = "v1"
  val v1DaxKey               = "v1-dax"
  val v2Key                  = "v2"
  val batchGetItemLimitKey   = "batch-get-item-limit"
  val batchWriteItemLimitKey = "batch-write-item-limit"

  def fromConfig(config: Config, legacy: Boolean): DynamoDBClientConfig = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientConfig(
      accessKeyId = config.getAs[String](accessKeyIdKeyKey),
      secretAccessKey = config.getAs[String](secretAccessKeyKey),
      endpoint = config.getAs[String](endpointKey),
      region = config.getAs[String](regionKey),
      clientVersion =
        config.getAs[String](clientVersionKey).map(s => ClientVersion.withName(s)).getOrElse(ClientVersion.V2),
      clientType = config.getAs[String](clientTypeKey).map(s => ClientType.withName(s)).getOrElse(ClientType.Async),
      DynamoDBClientV1Config.fromConfig(config.getOrElse[Config](v1Key, ConfigFactory.empty())),
      DynamoDBClientV1DaxConfig.fromConfig(config.getOrElse[Config](v1DaxKey, ConfigFactory.empty())), {
        if (legacy) {
          logger.warn(
            "<<<!!!CAUTION: PLEASE MIGRATE TO NEW CONFIG FORMAT!!!>>>\n" +
            "\tThe configuration items of AWS-SDK V2 client remain with the old key names: (j5ik2o.dynamo-db-journal.dynamo-db-client).\n" +
            "\tPlease change current key name to the new key name: (j5ik2o.dynamo-db-journal.dynamo-db-client.v2). \n\t" +
            DynamoDBClientV2Config.existsKeyNames(config).filter(_._2).keys.mkString("child-keys = [ ", ", ", " ]")
          )
          DynamoDBClientV2Config.fromConfig(config, legacy)
        } else
          DynamoDBClientV2Config.fromConfig(config.getOrElse[Config](v2Key, ConfigFactory.empty()), legacy)
      },
      batchGetItemLimit = config.getOrElse[Int](batchGetItemLimitKey, 100),
      batchWriteItemLimit = config.getOrElse[Int](batchWriteItemLimitKey, 25)
    )
    logger.debug("result = {}", result)
    result
  }

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
