/*
 * Copyright 2022 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.state.config

import com.github.j5ik2o.akka.persistence.dynamodb.const.DefaultColumnsDef
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config

object StateColumnsDefConfig extends LoggingSupport {
  val partitionKeyColumnNameKey       = "partition-key-column-name"
  val persistenceIdColumnNameKey      = "persistence-id-column-name"
  val revisionColumnNameKey           = "revision-nr-column-name"
  val deletedColumnNameKey            = "deleted-column-name"
  val payloadColumnNameKey            = "payload-column-name"
  val serializerIdColumnNameKey       = "serializer-id-column-name"
  val serializerManifestColumnNameKey = "serializer-manifest-column-name"
  val orderingColumnNameKey           = "ordering-column-name"
  val tagsColumnNameKey               = "tags-column-name"

  def fromConfig(config: Config): StateColumnsDefConfig = {
    logger.debug("config = {}", config)
    val result = StateColumnsDefConfig(
      sourceConfig = config,
      partitionKeyColumnName =
        config.valueAs[String](partitionKeyColumnNameKey, DefaultColumnsDef.PartitionKeyColumnName),
      persistenceIdColumnName =
        config.valueAs[String](persistenceIdColumnNameKey, DefaultColumnsDef.PersistenceIdColumnName),
      revisionColumnName = config.valueAs[String](revisionColumnNameKey, DefaultColumnsDef.RevisionNrColumnName),
      deletedColumnName = config.valueAs[String](deletedColumnNameKey, DefaultColumnsDef.DeletedColumnName),
      payloadColumnName = config.valueAs[String](payloadColumnNameKey, DefaultColumnsDef.PayloadColumnName),
      serializerIdColumnName =
        config.valueAs[String](serializerIdColumnNameKey, DefaultColumnsDef.SerializerIdColumnName),
      serializerManifestColumnName =
        config.valueAs[String](serializerManifestColumnNameKey, DefaultColumnsDef.SerializerManifestColumnName),
      orderingColumnName = config.valueAs[String](orderingColumnNameKey, DefaultColumnsDef.OrderingColumnName),
      tagsColumnName = config.valueAs[String](tagsColumnNameKey, DefaultColumnsDef.TagsColumnName)
    )
    logger.debug("result = {}", result)
    result
  }
}

final case class StateColumnsDefConfig(
    sourceConfig: Config,
    partitionKeyColumnName: String,
    persistenceIdColumnName: String,
    revisionColumnName: String,
    deletedColumnName: String,
    payloadColumnName: String,
    serializerIdColumnName: String,
    serializerManifestColumnName: String,
    orderingColumnName: String,
    tagsColumnName: String
)
