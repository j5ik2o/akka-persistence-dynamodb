package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.const.DefaultColumnsDef
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

object JournalColumnsDefConfig {

  def fromConfig(config: Config): JournalColumnsDefConfig = {
    JournalColumnsDefConfig(
      partitionKeyColumnName = config.asString("partition-key-column-name", DefaultColumnsDef.PartitionKey),
      persistenceIdColumnName = config.asString("persistence-id-column-name", DefaultColumnsDef.PersistenceIdColumnName),
      sequenceNrColumnName = config.asString("sequence-nr-column-name", DefaultColumnsDef.SequenceNrColumnName),
      deletedColumnName = config.asString("deleted-column-name", DefaultColumnsDef.DeletedColumnName),
      messageColumnName = config.asString("message-column-name", DefaultColumnsDef.MessageColumnName),
      orderingColumnName = config.asString("ordering-column-name", DefaultColumnsDef.OrderingColumnName),
      tagsColumnName = config.asString("tags-column-name", DefaultColumnsDef.TagsColumnName)
    )
  }

}
case class JournalColumnsDefConfig(partitionKeyColumnName: String,
                                   persistenceIdColumnName: String,
                                   sequenceNrColumnName: String,
                                   deletedColumnName: String,
                                   messageColumnName: String,
                                   orderingColumnName: String,
                                   tagsColumnName: String)
