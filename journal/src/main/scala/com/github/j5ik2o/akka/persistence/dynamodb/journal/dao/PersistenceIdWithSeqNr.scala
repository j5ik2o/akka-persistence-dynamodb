package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao

import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

case class PersistenceIdWithSeqNr(persistenceId: PersistenceId, sequenceNumber: SequenceNumber)
