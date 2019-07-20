package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.runtime.universe

trait MetricsReporter {
  def setPutMessagesDuration(value: Long): Unit
  def addPutMessagesCounter(value: Long): Unit
  def incrementPutMessagesCounter(): Unit = addPutMessagesCounter(1L)

  def setDeleteMessagesDuration(value: Long): Unit
  def addDeleteMessagesCounter(value: Long): Unit
  def incrementDeleteMessagesCounter(): Unit = addDeleteMessagesCounter(1L)

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

class DefaultMetricsReporter extends MetricsReporter {

  private val putMessagesDuration   = new AtomicLong()
  private val addPutMessagesCounter = new AtomicLong()

  override def setPutMessagesDuration(value: Long): Unit = putMessagesDuration.set(value)
  override def addPutMessagesCounter(value: Long): Unit  = addPutMessagesCounter.addAndGet(value)

  private val deleteMessagesDuration   = new AtomicLong()
  private val addDeleteMessagesCounter = new AtomicLong()

  override def setDeleteMessagesDuration(value: Long): Unit = deleteMessagesDuration.set(value)
  override def addDeleteMessagesCounter(value: Long): Unit  = addDeleteMessagesCounter.addAndGet(value)

  private val getJournalRowsDuration = new AtomicLong()
  private val getJournalRowsCounter  = new AtomicLong()

  override def setGetJournalRowsDuration(value: Long): Unit = getJournalRowsDuration.set(value)
  override def addGetJournalRowsCounter(value: Long): Unit  = getJournalRowsCounter.addAndGet(value)

  private val updateMessageDuration = new AtomicLong()
  private val updateMessageCounter  = new AtomicLong()

  override def setUpdateMessageDuration(value: Long): Unit = updateMessageDuration.set(value)
  override def addUpdateMessageCounter(value: Long): Unit  = updateMessageCounter.addAndGet(value)

  private val putJournalRowsDuration = new AtomicLong()
  private val putJournalRowsCounter  = new AtomicLong()

  override def setPutJournalRowsDuration(value: Long): Unit = putJournalRowsDuration.set(value)
  override def addPutJournalRowsCounter(value: Long): Unit  = putJournalRowsCounter.addAndGet(value)

  private val putJournalRowsTotalDuration = new AtomicLong()
  private val putJournalRowsTotalCounter  = new AtomicLong()

  override def setPutJournalRowsTotalDuration(value: Long): Unit = putJournalRowsTotalDuration.set(value)
  override def addPutJournalRowsTotalCounter(value: Long): Unit  = putJournalRowsTotalCounter.addAndGet(value)

  private val deleteJournalRowsDuration = new AtomicLong()
  private val deleteJournalRowsCounter  = new AtomicLong()

  override def setDeleteJournalRowsDuration(value: Long): Unit = deleteJournalRowsDuration.set(value)
  override def addDeleteJournalRowsCounter(value: Long): Unit  = deleteJournalRowsCounter.addAndGet(value)

  private val deleteJournalRowsTotalDuration = new AtomicLong()
  private val deleteJournalRowsTotalCounter  = new AtomicLong()

  override def setDeleteJournalRowsTotalDuration(value: Long): Unit = deleteJournalRowsTotalDuration.set(value)
  override def addDeleteJournalRowsTotalCounter(value: Long): Unit  = deleteJournalRowsTotalCounter.addAndGet(value)

  private val highestSequenceNrTotalDuration = new AtomicLong()
  private val highestSequenceNrTotalCounter  = new AtomicLong()

  override def setHighestSequenceNrTotalDuration(value: Long): Unit = highestSequenceNrTotalDuration.set(value)
  override def addHighestSequenceNrTotalCounter(value: Long): Unit  = highestSequenceNrTotalCounter.addAndGet(value)

  private val getMessagesDuration = new AtomicLong()
  private val getMessagesCounter  = new AtomicLong()

  override def setGetMessagesDuration(value: Long): Unit = getMessagesDuration.set(value)
  override def addGetMessagesCounter(value: Long): Unit  = getMessagesCounter.addAndGet(value)

  private val getMessagesTotalDuration = new AtomicLong()
  private val getMessagesTotalCounter  = new AtomicLong()

  override def setGetMessagesTotalDuration(value: Long): Unit = getMessagesTotalDuration.set(value)
  override def addGetMessagesTotalCounter(value: Long): Unit  = getMessagesTotalCounter.addAndGet(value)

  private val allPersistenceIdsDuration = new AtomicLong()
  private val allPersistenceIdsCounter  = new AtomicLong()

  override def setAllPersistenceIdsDuration(value: Long): Unit = allPersistenceIdsDuration.set(value)
  override def addAllPersistenceIdsCounter(value: Long): Unit  = allPersistenceIdsCounter.addAndGet(value)

  private val allPersistenceIdsTotalDuration = new AtomicLong()
  private val allPersistenceIdsTotalCounter  = new AtomicLong()

  override def setAllPersistenceIdsTotalDuration(value: Long): Unit = allPersistenceIdsTotalDuration.set(value)
  override def addAllPersistenceIdsTotalCounter(value: Long): Unit  = allPersistenceIdsTotalCounter.addAndGet(value)

  private val eventsByTagDuration = new AtomicLong()
  private val eventsByTagCounter  = new AtomicLong()

  override def setEventsByTagDuration(value: Long): Unit = eventsByTagDuration.set(value)
  override def addEventsByTagCounter(value: Long): Unit  = eventsByTagCounter.addAndGet(value)

  private val eventsByTagTotalDuration = new AtomicLong()
  private val eventsByTagTotalCounter  = new AtomicLong()

  override def setEventsByTagTotalDuration(value: Long): Unit = eventsByTagTotalDuration.set(value)
  override def addEventsByTagTotalCounter(value: Long): Unit  = eventsByTagTotalCounter.addAndGet(value)

  private val journalSequenceDuration = new AtomicLong()
  private val journalSequenceCounter  = new AtomicLong()

  override def setJournalSequenceDuration(value: Long): Unit = journalSequenceDuration.set(value)
  override def addJournalSequenceCounter(value: Long): Unit  = journalSequenceCounter.addAndGet(value)

  private val journalSequenceTotalDuration = new AtomicLong()
  private val journalSequenceTotalCounter  = new AtomicLong()

  override def setJournalSequenceTotalDuration(value: Long): Unit = journalSequenceTotalDuration.set(value)
  override def addJournalSequenceTotalCounter(value: Long): Unit  = journalSequenceTotalCounter.addAndGet(value)
}

class NullMetricsReporter extends MetricsReporter {
  override def setPutMessagesDuration(value: Long): Unit            = {}
  override def addPutMessagesCounter(value: Long): Unit             = {}
  override def setDeleteMessagesDuration(value: Long): Unit         = {}
  override def addDeleteMessagesCounter(value: Long): Unit          = {}
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
