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
package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.config

import com.github.j5ik2o.akka.persistence.dynamodb.const.DefaultColumnsDef
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config

object SnapshotColumnsDefConfig extends LoggingSupport {
  val partitionKeyColumnNameKey  = "partition-key-column-name"
  val sortKeyColumnNameKey       = "sort-key-column-name"
  val persistenceIdColumnNameKey = "persistence-id-column-name"
  val sequenceNrColumnNameKey    = "sequence-nr-column-name"
  val snapshotColumnNameKey      = "snapshot-column-name"
  val createdColumnNameKey       = "created-column-name"

  def fromConfig(config: Config): SnapshotColumnsDefConfig = {
    logger.debug("config = {}", config)
    val result = SnapshotColumnsDefConfig(
      sourceConfig = config,
      partitionKeyColumnName =
        config.valueAs[String](partitionKeyColumnNameKey, DefaultColumnsDef.PartitionKeyColumnName),
      sortKeyColumnName = config.valueAs[String](sortKeyColumnNameKey, DefaultColumnsDef.SortKeyColumnName),
      persistenceIdColumnName =
        config.valueAs[String](persistenceIdColumnNameKey, DefaultColumnsDef.PersistenceIdColumnName),
      sequenceNrColumnName = config.valueAs[String](sequenceNrColumnNameKey, DefaultColumnsDef.SequenceNrColumnName),
      snapshotColumnName = config.valueAs[String](snapshotColumnNameKey, DefaultColumnsDef.SnapshotColumnName),
      createdColumnName = config.valueAs[String](createdColumnNameKey, DefaultColumnsDef.CreatedColumnName)
    )
    logger.debug("result = {}", result)
    result
  }

}

case class SnapshotColumnsDefConfig(
    sourceConfig: Config,
    partitionKeyColumnName: String,
    sortKeyColumnName: String,
    persistenceIdColumnName: String,
    sequenceNrColumnName: String,
    snapshotColumnName: String,
    createdColumnName: String
)
