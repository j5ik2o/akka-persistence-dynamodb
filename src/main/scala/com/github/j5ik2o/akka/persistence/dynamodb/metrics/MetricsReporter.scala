package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import scala.reflect.runtime.universe

trait MetricsReporter {
  def setPutMessageDuration(value: Long): Unit
  def addPutMessageCounter(value: Long): Unit
  def incrementPutMessageCounter(): Unit = addPutMessageCounter(1L)

  def setDeleteMessageDuration(value: Long): Unit
  def addDeleteMessageCounter(value: Long): Unit
  def incrementDeleteMessageCounter(): Unit = addDeleteMessageCounter(1L)

  def setGetJournalRowsDuration(value: Long): Unit
  def addGetJournalRowsCounter(value: Long): Unit
  def incrementGetJournalRowsCounter(): Unit = addGetJournalRowsCounter(1L)

  def setUpdateMessageDuration(value: Long): Unit
  def addUpdateMessageCounter(value: Long): Unit
  def incrementUpdateMessageCounter(): Unit = addUpdateMessageCounter(1L)

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
  override def setPutMessageDuration(value: Long): Unit             = {}
  override def addPutMessageCounter(value: Long): Unit              = {}
  override def setDeleteMessageDuration(value: Long): Unit          = {}
  override def addDeleteMessageCounter(value: Long): Unit           = {}
  override def setGetJournalRowsDuration(value: Long): Unit         = {}
  override def addGetJournalRowsCounter(value: Long): Unit          = {}
  override def setUpdateMessageDuration(value: Long): Unit          = {}
  override def addUpdateMessageCounter(value: Long): Unit           = {}
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
