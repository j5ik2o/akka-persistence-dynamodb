package com.github.j5ik2o.akka.persistence.dynamodb.journal

import monix.eval.Task

trait JournalDaoWithUpdates extends JournalDao {

  def update(persistenceId: String, sequenceNr: Long, payload: AnyRef): Task[Unit]

}
