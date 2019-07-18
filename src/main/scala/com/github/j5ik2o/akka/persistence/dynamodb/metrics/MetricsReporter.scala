package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import scala.reflect.runtime.universe

trait MetricsReporter {
  def setPutJournalRowsDuration(value: Long): Unit
  def addPutJournalRowsCounter(value: Long): Unit
  def incrementPutJournalRowsCounter(): Unit = addPutJournalRowsCounter(1L)

  def setPutJournalRowsTotalDuration(value: Long): Unit
  def addPutJournalRowsTotalCounter(value: Long)
  def incrementPutJournalRowsTotalCounter(): Unit = addPutJournalRowsTotalCounter(1L)

  def setDeleteJournalRowsDuration(value: Long): Unit
  def addDeleteJournalRowsCounter(value: Long): Unit
  def incrementDeleteJournalRowsCounter(): Unit = addDeleteJournalRowsCounter(1L)

  def setDeleteJournalRowsTotalDuration(value: Long): Unit
  def addDeleteJournalRowsTotalCounter(value: Long): Unit
  def incrementDeleteJournalRowsTotalCounter(): Unit = addDeleteJournalRowsTotalCounter(1L)

  def setHighestSequenceNrTotalDuration(value: Long): Unit
  def addHighestSequenceNrTotalCounter(value: Long): Unit
  def incrementHighestSequenceNrTotalCounter(): Unit = addHighestSequenceNrTotalCounter(1L)

  def setGetMessagesDuration(value: Long): Unit
  def addGetMessagesCounter(value: Long): Unit
  def incrementGetMessagesCounter(): Unit = addGetMessagesCounter(1L)

  def setGetMessagesTotalDuration(value: Long): Unit
  def addGetMessagesTotalCounter(value: Long): Unit
  def incrementGetMessagesTotalCounter(): Unit = addGetMessagesTotalCounter(1L)

  def setAllPersistenceIdsDuration(value: Long): Unit
  def addAllPersistenceIdsCounter(value: Long): Unit
  def incrementAllPersistenceIdsCounter(): Unit = addAllPersistenceIdsCounter(1L)

  def setAllPersistenceIdsTotalDuration(value: Long): Unit
  def addAllPersistenceIdsTotalCounter(value: Long): Unit
  def incrementAllPersistenceIdsTotalCounter(): Unit = addAllPersistenceIdsTotalCounter(1L)

  def setEventsByTagDuration(value: Long): Unit
  def addEventsByTagCounter(value: Long): Unit
  def incrementEventsByTagCounter(): Unit = addEventsByTagCounter(1L)

  def setEventsByTagTotalDuration(value: Long): Unit
  def addEventsByTagTotalCounter(value: Long): Unit
  def incrementEventsByTagTotalCounter(): Unit = addEventsByTagTotalCounter(1L)

  def setJournalSequenceDuration(value: Long): Unit
  def addJournalSequenceCounter(value: Long): Unit
  def incrementJournalSequenceCounter(): Unit = addJournalSequenceCounter(1L)

  def setJournalSequenceTotalDuration(value: Long): Unit
  def addJournalSequenceTotalCounter(value: Long): Unit
  def incrementJournalSequenceTotalCounter(): Unit = addJournalSequenceTotalCounter(1L)
}

object MetricsReporter {

  def create(className: String): MetricsReporter = {
    val runtimeMirror     = universe.runtimeMirror(getClass.getClassLoader)
    val classSymbol       = runtimeMirror.staticClass(className)
    val classMirror       = runtimeMirror.reflectClass(classSymbol)
    val constructorMethod = classSymbol.typeSignature.decl(universe.termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructorMethod)
    val newInstance       = constructorMirror()
    newInstance.asInstanceOf[MetricsReporter]
  }

}

class NullMetricsReporter extends MetricsReporter {
  override def setPutJournalRowsDuration(value: Long): Unit         = {}
  override def addPutJournalRowsCounter(value: Long): Unit          = {}
  override def setPutJournalRowsTotalDuration(value: Long): Unit    = {}
  override def addPutJournalRowsTotalCounter(value: Long): Unit     = {}
  override def setDeleteJournalRowsDuration(value: Long): Unit      = {}
  override def addDeleteJournalRowsCounter(value: Long): Unit       = {}
  override def setDeleteJournalRowsTotalDuration(value: Long): Unit = {}
  override def addDeleteJournalRowsTotalCounter(value: Long): Unit  = {}
  override def setHighestSequenceNrTotalDuration(value: Long): Unit = {}
  override def addHighestSequenceNrTotalCounter(value: Long): Unit  = {}
  override def setGetMessagesDuration(value: Long): Unit            = {}
  override def addGetMessagesCounter(value: Long): Unit             = {}
  override def setGetMessagesTotalDuration(value: Long): Unit       = {}
  override def addGetMessagesTotalCounter(value: Long): Unit        = {}
  override def setAllPersistenceIdsDuration(value: Long): Unit      = {}
  override def addAllPersistenceIdsCounter(value: Long): Unit       = {}
  override def setAllPersistenceIdsTotalDuration(value: Long): Unit = {}
  override def addAllPersistenceIdsTotalCounter(value: Long): Unit  = {}
  override def setEventsByTagDuration(value: Long): Unit            = {}
  override def addEventsByTagCounter(value: Long): Unit             = {}
  override def setEventsByTagTotalDuration(value: Long): Unit       = {}
  override def addEventsByTagTotalCounter(value: Long): Unit        = {}
  override def setJournalSequenceDuration(value: Long): Unit        = {}
  override def addJournalSequenceCounter(value: Long): Unit         = {}
  override def setJournalSequenceTotalDuration(value: Long): Unit   = {}
  override def addJournalSequenceTotalCounter(value: Long): Unit    = {}
}
