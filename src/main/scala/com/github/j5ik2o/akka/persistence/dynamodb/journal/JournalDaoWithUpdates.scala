package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.Done

import scala.concurrent.Future

trait JournalDaoWithUpdates extends JournalDao {

  def update(persistenceId: String, sequenceNr: Long, payload: AnyRef): Future[Done]

}
