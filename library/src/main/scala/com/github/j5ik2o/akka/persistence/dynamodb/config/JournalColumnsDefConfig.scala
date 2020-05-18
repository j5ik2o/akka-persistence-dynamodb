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

import com.github.j5ik2o.akka.persistence.dynamodb.const.DefaultColumnsDef
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

object JournalColumnsDefConfig extends LoggingSupport {

  val partitionKeyColumnNameKey  = "partition-key-column-name"
  val sortKeyColumnNameKey       = "sort-key-column-name"
  val persistenceIdColumnNameKey = "persistence-id-column-name"
  val sequenceNrColumnNameKey    = "sequence-nr-column-name"
  val deletedColumnNameKey       = "deleted-column-name"
  val messageColumnNameKey       = "message-column-name"
  val orderingColumnNameKey      = "ordering-column-name"
  val tagsColumnNameKey          = "tags-column-name"

  def fromConfig(config: Config): JournalColumnsDefConfig = {
    logger.debug("config = {}", config)
    val result = JournalColumnsDefConfig(
      partitionKeyColumnName =
        config.getOrElse[String](partitionKeyColumnNameKey, DefaultColumnsDef.PartitionKeyColumnName),
      sortKeyColumnName = config.getOrElse[String](sortKeyColumnNameKey, DefaultColumnsDef.SortKeyColumnName),
      persistenceIdColumnName =
        config.getOrElse[String](persistenceIdColumnNameKey, DefaultColumnsDef.PersistenceIdColumnName),
      sequenceNrColumnName = config.getOrElse[String](sequenceNrColumnNameKey, DefaultColumnsDef.SequenceNrColumnName),
      deletedColumnName = config.getOrElse[String](deletedColumnNameKey, DefaultColumnsDef.DeletedColumnName),
      messageColumnName = config.getOrElse[String](messageColumnNameKey, DefaultColumnsDef.MessageColumnName),
      orderingColumnName = config.getOrElse[String](orderingColumnNameKey, DefaultColumnsDef.OrderingColumnName),
      tagsColumnName = config.getOrElse[String](tagsColumnNameKey, DefaultColumnsDef.TagsColumnName)
    )
    logger.debug("result = {}", result)
    result
  }

}

case class JournalColumnsDefConfig(
    partitionKeyColumnName: String,
    sortKeyColumnName: String,
    persistenceIdColumnName: String,
    sequenceNrColumnName: String,
    deletedColumnName: String,
    messageColumnName: String,
    orderingColumnName: String,
    tagsColumnName: String
)
