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

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
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
      partitionKeyColumnName = config.value[String](partitionKeyColumnNameKey),
      sortKeyColumnName = config.value[String](sortKeyColumnNameKey),
      persistenceIdColumnName = config.value[String](persistenceIdColumnNameKey),
      sequenceNrColumnName = config.value[String](sequenceNrColumnNameKey),
      deletedColumnName = config.value[String](deletedColumnNameKey),
      messageColumnName = config.value[String](messageColumnNameKey),
      orderingColumnName = config.value[String](orderingColumnNameKey),
      tagsColumnName = config.value[String](tagsColumnNameKey)
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
