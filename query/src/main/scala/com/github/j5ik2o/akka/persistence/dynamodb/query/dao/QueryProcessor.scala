package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.{ Source, SourceUtils }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId }

trait QueryProcessor {
  def allPersistenceIds(max: Long): Source[PersistenceId, NotUsed]

  def eventsByTagAsJournalRow(
      tag: String,
      offset: Long,
      maxOffset: Long,
      max: Long
  ): Source[JournalRow, NotUsed]

  def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed]

  protected val startTimeSource: Source[Long, NotUsed] =
    SourceUtils
      .lazySource(() => Source.single(System.nanoTime())).mapMaterializedValue(_ => NotUsed)

  protected val logLevels: Attributes = Attributes.logLevels(
    onElement = Attributes.LogLevels.Debug,
    onFailure = Attributes.LogLevels.Error,
    onFinish = Attributes.LogLevels.Debug
  )

}
