package com.github.j5ik2o.akka.persistence.dynamodb.jmx

import java.util.concurrent.atomic.AtomicLong

trait JournalDaoStatusMBean {
  def getMessagesTotalDuration: Long
  def getMessagesTotalCounter: Long
  def getMessagesDuration: Long
  def getMessagesCounter: Long

  def getPutJournalRowsTotalDuration: Long
  def getPutJournalRowsTotalCounter: Long
  def getPutJournalRowsDuration: Long
  def getPutJournalRowsCounter: Long

  def getDeleteJournalRowsTotalDuration: Long
  def getDeleteJournalRowsTotalCounter: Long
  def getDeleteJournalRowsDuration: Long
  def getDeleteJournalRowsCounter: Long

  def getHighestSequenceNrTotalDuration: Long
  def getHighestSequenceNrTotalCounter: Long
}

class JournalDaoStatus extends JournalDaoStatusMBean {

  private val messagesDuration      = new AtomicLong()
  private val messagesCounter       = new AtomicLong()
  private val messagesTotalDuration = new AtomicLong()
  private val messagesTotalCounter  = new AtomicLong()

  private val putJournalRowsTotalDuration = new AtomicLong()
  private val putJournalRowsTotalCounter  = new AtomicLong()
  private val putJournalRowsDuration      = new AtomicLong()
  private val putJournalRowsCounter       = new AtomicLong()

  private val deleteJournalRowsTotalDuration = new AtomicLong()
  private val deleteJournalRowsTotalCounter  = new AtomicLong()
  private val deleteJournalRowsDuration      = new AtomicLong()
  private val deleteJournalRowsCounter       = new AtomicLong()

  private val highestSequenceNrDuration = new AtomicLong()
  private val highestSequenceNrCounter  = new AtomicLong()

  def setMessagesTotalDuration(value: Long): Unit = messagesTotalDuration.set(value)
  def addMessagesTotalCounter(value: Long): Unit  = messagesTotalCounter.addAndGet(value)
  def incrementMessagesTotalCounter(): Unit       = addMessagesTotalCounter(1L)

  override def getMessagesTotalDuration: Long = messagesTotalDuration.get
  override def getMessagesTotalCounter: Long  = messagesTotalCounter.get

  def setMessagesDuration(value: Long): Unit = messagesDuration.set(value)
  def addMessagesCounter(value: Long): Unit  = messagesCounter.addAndGet(value)
  def incrementMessagesCounter(): Unit       = addMessagesTotalCounter(1L)

  override def getMessagesDuration: Long = messagesDuration.get
  override def getMessagesCounter: Long  = messagesCounter.get

  def setPutJournalRowsTotalDuration(value: Long): Unit = putJournalRowsTotalDuration.set(value)
  def addPutJournalRowsTotalCounter(value: Long): Unit  = putJournalRowsTotalCounter.addAndGet(value)
  def incrementPutJournalRowsTotalCounter(): Unit       = addPutJournalRowsTotalCounter(1L)

  override def getPutJournalRowsTotalDuration: Long = putJournalRowsTotalDuration.get
  override def getPutJournalRowsTotalCounter: Long  = putJournalRowsTotalCounter.get

  def setPutJournalRowsDuration(value: Long): Unit = putJournalRowsDuration.set(value)
  def addPutJournalRowsCounter(value: Long): Unit  = putJournalRowsCounter.addAndGet(value)
  def incrementPutJournalRowsCounter(): Unit       = addPutJournalRowsCounter(1L)

  override def getPutJournalRowsDuration: Long = putJournalRowsDuration.get
  override def getPutJournalRowsCounter: Long  = putJournalRowsCounter.get

  def setDeleteJournalRowsTotalDuration(value: Long): Unit = deleteJournalRowsTotalDuration.set(value)
  def addDeleteJournalRowsTotalCounter(value: Long): Unit  = deleteJournalRowsTotalCounter.addAndGet(value)
  def incrementDeleteJournalRowsTotalCounter(): Unit       = addDeleteJournalRowsTotalCounter(1L)

  override def getDeleteJournalRowsTotalDuration: Long = deleteJournalRowsTotalDuration.get
  override def getDeleteJournalRowsTotalCounter: Long  = deleteJournalRowsTotalCounter.get

  def setDeleteJournalRowsDuration(value: Long): Unit = deleteJournalRowsDuration.set(value)
  def addDeleteJournalRowsCounter(value: Long): Unit  = deleteJournalRowsCounter.addAndGet(value)
  def incrementDeleteJournalRowsCounter(): Unit       = addDeleteJournalRowsCounter(1L)

  override def getDeleteJournalRowsDuration: Long = deleteJournalRowsDuration.get
  override def getDeleteJournalRowsCounter: Long  = deleteJournalRowsCounter.get

  def setHighestSequenceNrTotalDuration(value: Long): Unit = highestSequenceNrDuration.set(value)
  def addHighestSequenceNrTotalCounter(value: Long): Unit  = highestSequenceNrCounter.addAndGet(value)
  def incrementHighestSequenceNrTotalCounter(): Unit       = addHighestSequenceNrTotalCounter(1L)

  override def getHighestSequenceNrTotalDuration: Long = highestSequenceNrDuration.get
  override def getHighestSequenceNrTotalCounter: Long  = highestSequenceNrCounter.get
}

trait QueryDaoStatusMBean {
  def getMessagesTotalDuration: Long
  def getMessagesTotalCounter: Long
  def getMessagesDuration: Long
  def getMessagesCounter: Long

  def getAllPersistenceIdsTotalDuration: Long
  def getAllPersistenceIdsTotalCounter: Long
  def getAllPersistenceIdsDuration: Long
  def getAllPersistenceIdsCounter: Long

  def getEventsByTagDuration: Long
  def getEventsByTagCounter: Long
  def getEventsByTagTotalDuration: Long
  def getEventsByTagTotalCounter: Long

  def getJournalSequenceDuration: Long
  def getJournalSequenceCounter: Long
  def getJournalSequenceTotalDuration: Long
  def getJournalSequenceTotalCounter: Long
}

class QueryDaoStatus extends QueryDaoStatusMBean {

  private val messagesDuration      = new AtomicLong()
  private val messagesCounter       = new AtomicLong()
  private val messagesTotalDuration = new AtomicLong()
  private val messagesTotalCounter  = new AtomicLong()

  private val allPersistenceIdsDuration      = new AtomicLong()
  private val allPersistenceIdsCounter       = new AtomicLong()
  private val allPersistenceIdsTotalDuration = new AtomicLong()
  private val allPersistenceIdsTotalCounter  = new AtomicLong()

  private val eventsByTagDuration      = new AtomicLong()
  private val eventsByTagCounter       = new AtomicLong()
  private val eventsByTagTotalDuration = new AtomicLong()
  private val eventsByTagTotalCounter  = new AtomicLong()

  private val journalSequenceDuration      = new AtomicLong()
  private val journalSequenceCounter       = new AtomicLong()
  private val journalSequenceTotalDuration = new AtomicLong()
  private val journalSequenceTotalCounter  = new AtomicLong()

  def setMessagesTotalDuration(value: Long): Unit = messagesTotalDuration.set(value)
  def addMessagesTotalCounter(value: Long): Unit  = messagesTotalCounter.addAndGet(value)
  def incrementMessagesTotalCounter(): Unit       = addMessagesTotalCounter(1)

  def setMessagesDuration(value: Long): Unit = messagesDuration.set(value)
  def addMessagesCounter(value: Long): Unit  = messagesCounter.addAndGet(value)
  def incrementMessagesCounter(): Unit       = addMessagesCounter(1)

  override def getMessagesTotalDuration: Long = messagesTotalDuration.get()
  override def getMessagesTotalCounter: Long  = messagesTotalCounter.get()
  override def getMessagesDuration: Long      = messagesDuration.get()
  override def getMessagesCounter: Long       = messagesCounter.get()

  def setAllPersistenceIdsTotalDuration(value: Long): Unit = allPersistenceIdsTotalDuration.set(value)
  def addAllPersistenceIdsTotalCounter(value: Long): Unit  = allPersistenceIdsTotalCounter.addAndGet(value)
  def incrementAllPersistenceIdsTotalCounter(): Unit       = addAllPersistenceIdsTotalCounter(1)

  def setAllPersistenceIdsDuration(value: Long): Unit = allPersistenceIdsDuration.set(value)
  def addAllPersistenceIdsCounter(value: Long): Unit  = allPersistenceIdsCounter.addAndGet(value)
  def incrementAllPersistenceIdsCounter(): Unit       = addAllPersistenceIdsCounter(1)

  override def getAllPersistenceIdsTotalDuration: Long = allPersistenceIdsTotalDuration.get()
  override def getAllPersistenceIdsTotalCounter: Long  = allPersistenceIdsTotalCounter.get()
  override def getAllPersistenceIdsDuration: Long      = allPersistenceIdsDuration.get()
  override def getAllPersistenceIdsCounter: Long       = allPersistenceIdsCounter.get()

  def setEventsByTagTotalDuration(value: Long): Unit = eventsByTagTotalDuration.set(value)
  def addEventsByTagTotalCounter(value: Long): Unit  = eventsByTagTotalCounter.addAndGet(value)
  def incrementEventsByTagTotalCounter(): Unit       = addEventsByTagTotalCounter(1)

  def setEventsByTagDuration(value: Long): Unit = eventsByTagTotalDuration.set(value)
  def setEventsByTagCounter(value: Long): Unit  = eventsByTagCounter.addAndGet(value)
  def incrementEventsByTagCounter(): Unit       = setEventsByTagCounter(1)

  override def getEventsByTagTotalDuration: Long = eventsByTagTotalDuration.get()
  override def getEventsByTagTotalCounter: Long  = eventsByTagTotalCounter.get()
  override def getEventsByTagDuration: Long      = eventsByTagDuration.get()
  override def getEventsByTagCounter: Long       = eventsByTagCounter.get()

  def setJournalSequenceDuration(value: Long): Unit = journalSequenceDuration.set(value)
  def addJournalSequenceCounter(value: Long): Unit  = journalSequenceCounter.addAndGet(value)
  def incrementJournalSequenceCounter(): Unit       = addJournalSequenceCounter(1)

  def setJournalSequenceTotalDuration(value: Long): Unit = journalSequenceTotalDuration.set(value)
  def addJournalSequenceTotalCounter(value: Long): Unit  = journalSequenceTotalCounter.addAndGet(value)
  def incrementJournalSequenceTotalCounter(): Unit       = addJournalSequenceTotalCounter(1)

  override def getJournalSequenceDuration: Long      = journalSequenceDuration.get()
  override def getJournalSequenceCounter: Long       = journalSequenceCounter.get()
  override def getJournalSequenceTotalDuration: Long = journalSequenceTotalDuration.get()
  override def getJournalSequenceTotalCounter: Long  = journalSequenceTotalCounter.get()
}
