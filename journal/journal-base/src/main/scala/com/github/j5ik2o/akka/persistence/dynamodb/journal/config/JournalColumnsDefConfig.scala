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
package com.github.j5ik2o.akka.persistence.dynamodb.journal.config

import net.ceedubs.ficus.Ficus._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config

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
      sourceConfig = config,
      partitionKeyColumnName = config.as[String](partitionKeyColumnNameKey),
      sortKeyColumnName = config.as[String](sortKeyColumnNameKey),
      persistenceIdColumnName = config.as[String](persistenceIdColumnNameKey),
      sequenceNrColumnName = config.as[String](sequenceNrColumnNameKey),
      deletedColumnName = config.as[String](deletedColumnNameKey),
      messageColumnName = config.as[String](messageColumnNameKey),
      orderingColumnName = config.as[String](orderingColumnNameKey),
      tagsColumnName = config.as[String](tagsColumnNameKey)
    )
    logger.debug("result = {}", result)
    result
  }

}

final case class JournalColumnsDefConfig(
    sourceConfig: Config,
    partitionKeyColumnName: String,
    sortKeyColumnName: String,
    persistenceIdColumnName: String,
    sequenceNrColumnName: String,
    deletedColumnName: String,
    messageColumnName: String,
    orderingColumnName: String,
    tagsColumnName: String
)
