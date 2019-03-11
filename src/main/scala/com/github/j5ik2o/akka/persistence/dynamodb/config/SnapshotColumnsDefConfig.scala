package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.DefaultColumnsDef
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

object SnapshotColumnsDefConfig {

  def fromConfig(config: Config): SnapshotColumnsDefConfig = {
    SnapshotColumnsDefConfig(
      persistenceIdColumnName = config.asString("persistence-id-column-name", DefaultColumnsDef.PersistenceIdColumnName),
      sequenceNrColumnName = config.asString("sequence-nr-column-name", DefaultColumnsDef.SequenceNrColumnName),
      snapshotColumnName = config.asString("snapshot-column-name", DefaultColumnsDef.SnapshotColumnName),
      createdColumnName = config.asString("created-column-name", DefaultColumnsDef.CreatedColumnName)
    )
  }

}

case class SnapshotColumnsDefConfig(persistenceIdColumnName: String,
                                    sequenceNrColumnName: String,
                                    snapshotColumnName: String,
                                    createdColumnName: String)
