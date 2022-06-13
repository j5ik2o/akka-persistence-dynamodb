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
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.v2dax.DynamoDBClientV2DaxConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.{ Config, ConfigFactory }

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
  val v2DaxKey               = "v2-dax"
  val batchGetItemLimitKey   = "batch-get-item-limit"
  val batchWriteItemLimitKey = "batch-write-item-limit"

  val DefaultClientVersion: ClientVersion.Value = ClientVersion.V2
  val DefaultClientType: ClientType.Value       = ClientType.Async
  val DefaultBatchGetItemLimit                  = 100
  val DefaultBatchWriteItemLimit                = 25

  def fromConfig(config: Config, legacyConfigFormat: Boolean): DynamoDBClientConfig = {
    logger.debug("config = {}", config)
    val clientVersion =
      config.valueOptAs[String](clientVersionKey).map(s => ClientVersion.withName(s)).getOrElse(DefaultClientVersion)
    val result = DynamoDBClientConfig(
      sourceConfig = config,
      accessKeyId = config.valueOptAs(accessKeyIdKeyKey),
      secretAccessKey = config.valueOptAs(secretAccessKeyKey),
      endpoint = config.valueOptAs(endpointKey),
      region = config.valueOptAs(regionKey),
      clientVersion,
      clientType =
        config.valueOptAs[String](clientTypeKey).map(s => ClientType.withName(s)).getOrElse(DefaultClientType),
      DynamoDBClientV1Config
        .fromConfig(config.configAs(v1Key, ConfigFactory.empty()), clientVersion == ClientVersion.V1),
      DynamoDBClientV1DaxConfig
        .fromConfig(config.configAs(v1DaxKey, ConfigFactory.empty()), clientVersion == ClientVersion.V1Dax), {
        if (legacyConfigFormat) {
          logger.warn(
            "<<<!!!CAUTION: PLEASE MIGRATE TO NEW CONFIG FORMAT!!!>>>\n" +
            "\tThe configuration items of AWS-SDK V2 client remain with the old key names: (j5ik2o.dynamo-db-journal.dynamo-db-client).\n" +
            "\tPlease change current key name to the new key name: (j5ik2o.dynamo-db-journal.dynamo-db-client.v2). \n\t" +
            DynamoDBClientV2Config.existsKeyNames(config).filter(_._2).keys.mkString("child-keys = [ ", ", ", " ]")
          )
          DynamoDBClientV2Config.fromConfig(config, clientVersion == ClientVersion.V2, legacyConfigFormat)
        } else
          DynamoDBClientV2Config.fromConfig(
            config.configAs(v2Key, ConfigFactory.empty()),
            clientVersion == ClientVersion.V2,
            legacyConfigFormat
          )
      },
      DynamoDBClientV2DaxConfig
        .fromConfig(config.configAs(v2DaxKey, ConfigFactory.empty()), clientVersion == ClientVersion.V2),
      batchGetItemLimit = config.valueAs(batchGetItemLimitKey, DefaultBatchGetItemLimit),
      batchWriteItemLimit = config.valueAs(batchWriteItemLimitKey, DefaultBatchWriteItemLimit)
    )
    logger.debug("result = {}", result)
    result
  }

}

final case class DynamoDBClientConfig(
    sourceConfig: Config,
    accessKeyId: Option[String],
    secretAccessKey: Option[String],
    endpoint: Option[String],
    region: Option[String],
    clientVersion: ClientVersion.Value,
    clientType: ClientType.Value,
    v1ClientConfig: DynamoDBClientV1Config,
    v1DaxClientConfig: DynamoDBClientV1DaxConfig,
    v2ClientConfig: DynamoDBClientV2Config,
    v2DaxClientConfig: DynamoDBClientV2DaxConfig,
    batchGetItemLimit: Int, // Currently unused
    batchWriteItemLimit: Int
) {
  require(batchGetItemLimit >= 1 && batchGetItemLimit <= 100)
  require(batchWriteItemLimit >= 1 && batchWriteItemLimit <= 25)
}
