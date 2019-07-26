package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import scala.reflect.runtime.universe

trait MetricsReporter {
  def setAsyncWriteMessagesCallDuration(value: Long): Unit
  def addAsyncWriteMessagesCallCounter(value: Long): Unit
  def incrementAsyncWriteMessagesCallCounter(): Unit = addAsyncWriteMessagesCallCounter(1L)
  def addAsyncWriteMessagesCallErrorCounter(value: Long): Unit
  def incrementAsyncWriteMessagesCallErrorCounter(): Unit = addAsyncWriteMessagesCallErrorCounter(1L)

  def setAsyncDeleteMessagesToCallDuration(value: Long): Unit
  def addAsyncDeleteMessagesToCallCounter(value: Long): Unit
  def incrementAsyncDeleteMessagesToCallCounter(): Unit = addAsyncDeleteMessagesToCallCounter(1L)
  def addAsyncDeleteMessagesToCallErrorCounter(value: Long): Unit
  def incrementAsyncDeleteMessagesToCallErrorCounter(): Unit = addAsyncDeleteMessagesToCallErrorCounter(1L)

  def setAsyncReplayMessagesCallDuration(value: Long): Unit
  def addAsyncReplayMessagesCallCounter(value: Long): Unit
  def incrementAsyncReplayMessagesCallCounter(): Unit = addAsyncReplayMessagesCallCounter(1L)
  def addAsyncReplayMessagesCallErrorCounter(value: Long): Unit
  def incrementAsyncReplayMessagesCallErrorCounter(): Unit = addAsyncReplayMessagesCallErrorCounter(1L)

  def setAsyncReadHighestSequenceNrCallDuration(value: Long): Unit
  def addAsyncReadHighestSequenceNrCallCounter(value: Long): Unit
  def incrementAsyncReadHighestSequenceNrCallCounter(): Unit = addAsyncReadHighestSequenceNrCallCounter(1L)
  def addAsyncReadHighestSequenceNrCallErrorCounter(value: Long): Unit
  def incrementAsyncReadHighestSequenceNrCallErrorCounter(): Unit = addAsyncReadHighestSequenceNrCallErrorCounter(1L)

  def addPutMessagesEnqueueCounter(value: Long): Unit
  def incrementPutMessagesEnqueueCounter(): Unit = addPutMessagesEnqueueCounter(1L)
  def addPutMessagesDequeueCounter(value: Long): Unit
  def incrementPutMessagesDequeueCounter(): Unit = addPutMessagesDequeueCounter(1L)

  def addDeleteMessagesEnqueueCounter(value: Long): Unit
  def incrementDeleteMessagesEnqueueCounter(): Unit = addDeleteMessagesEnqueueCounter(1L)
  def addDeleteMessagesDequeueCounter(value: Long): Unit
  def incrementDeleteMessagesDequeueCounter(): Unit = addDeleteMessagesDequeueCounter(1L)

  def setPutMessagesCallDuration(value: Long): Unit
  def addPutMessagesCallCounter(value: Long): Unit
  def incrementPutMessagesCallCounter(): Unit = addPutMessagesCallCounter(1L)
  def addPutMessagesCallErrorCounter(value: Long): Unit
  def incrementPutMessagesCallErrorCounter(): Unit = addPutMessagesCallErrorCounter(1L)

  def setDeleteMessagesCallDuration(value: Long): Unit
  def addDeleteMessagesCallCounter(value: Long): Unit
  def incrementDeleteMessagesCallCounter(): Unit = addDeleteMessagesCallCounter(1L)
  def addDeleteMessagesCallErrorCounter(value: Long): Unit
  def incrementDeleteMessagesCallErrorCounter(): Unit = addDeleteMessagesCallErrorCounter(1L)

  def setGetJournalRowsItemDuration(value: Long): Unit
  def addGetJournalRowsItemCounter(value: Long): Unit
  def incrementGetJournalRowsItemCounter(): Unit = addGetJournalRowsItemCounter(1L)
  def addGetJournalRowsItemCallCounter(value: Long): Unit
  def incrementGetJournalRowsItemCallCounter(): Unit = addGetJournalRowsItemCallCounter(1L)
  def addGetJournalRowsItemCallErrorCounter(value: Long): Unit
  def incrementGetJournalRowsItemCallErrorCounter(): Unit = addGetJournalRowsItemCallErrorCounter(1L)

  def setUpdateMessageCallDuration(value: Long): Unit
  def addUpdateMessageCallCounter(value: Long): Unit
  def incrementUpdateMessageCallCounter(): Unit = addUpdateMessageCallCounter(1L)
  def addUpdateMessageCallErrorCounter(value: Long): Unit
  def incrementUpdateMessageCallErrorCounter(): Unit = addUpdateMessageCallErrorCounter(1L)

  def setPutJournalRowsItemDuration(value: Long): Unit
  def addPutJournalRowsItemCounter(value: Long): Unit
  def incrementPutJournalRowsItemCounter(): Unit = addPutJournalRowsItemCounter(1L)
  def addPutJournalRowsItemCallCounter(value: Long): Unit
  def addPutJournalRowsItemCallCounter(): Unit = addPutJournalRowsItemCallCounter(1L)
  def addPutJournalRowsItemCallErrorCounter(value: Long): Unit
  def incrementPutJournalRowsItemCallErrorCounter(): Unit = addPutJournalRowsItemCallErrorCounter(1L)
  def setPutJournalRowsCallDuration(value: Long): Unit
  def addPutJournalRowsCallCounter(value: Long): Unit
  def incrementPutJournalRowsCallCounter(): Unit = addPutJournalRowsCallCounter(1L)
  def addPutJournalRowsCallErrorCounter(value: Long): Unit
  def incrementPutJournalRowsCallErrorCounter(): Unit = addPutJournalRowsCallErrorCounter(1L)

  def setDeleteJournalRowsItemDuration(value: Long): Unit
  def addDeleteJournalRowsItemCounter(value: Long): Unit
  def incrementDeleteJournalRowsItemCounter(): Unit = addDeleteJournalRowsItemCounter(1L)
  def addDeleteJournalRowsItemCallCounter(value: Long): Unit
  def incrementDeleteJournalRowsItemCallCounter(): Unit = addDeleteJournalRowsItemCallCounter(1L)
  def addDeleteJournalRowsItemCallErrorCounter(value: Long): Unit
  def incrementDeleteJournalRowsItemCallErrorCounter(): Unit = addDeleteJournalRowsItemCallErrorCounter(1L)
  def setDeleteJournalRowsCallDuration(value: Long): Unit
  def addDeleteJournalRowsCallCounter(value: Long): Unit
  def incrementDeleteJournalRowsCallCounter(): Unit = addDeleteJournalRowsCallCounter(1L)
  def addDeleteJournalRowsCallErrorCounter(value: Long): Unit
  def incrementDeleteJournalRowsCallErrorCounter(): Unit = addDeleteJournalRowsCallErrorCounter(1L)

  def setHighestSequenceNrItemDuration(value: Long): Unit
  def addHighestSequenceNrItemCounter(value: Long): Unit
  def incrementHighestSequenceNrItemCounter(): Unit = addHighestSequenceNrItemCounter(1L)
  def addHighestSequenceNrItemCallCounter(value: Long): Unit
  def incrementHighestSequenceNrItemCallCounter(): Unit = addHighestSequenceNrItemCallCounter(1L)
  def addHighestSequenceNrItemCallErrorCounter(value: Long): Unit
  def incrementHighestSequenceNrItemCallErrorCounter(): Unit = addHighestSequenceNrItemCallErrorCounter(1L)
  def setHighestSequenceNrCallDuration(value: Long): Unit
  def addHighestSequenceNrCallCounter(value: Long): Unit
  def incrementHighestSequenceNrCallCounter(): Unit = addHighestSequenceNrCallCounter(1L)
  def setHighestSequenceNrCallErrorCounter(value: Long): Unit
  def incrementHighestSequenceNrCallErrorCounter(): Unit = setHighestSequenceNrCallErrorCounter(1L)

  def setGetMessagesItemDuration(value: Long): Unit
  def addGetMessagesItemCounter(value: Long): Unit
  def incrementGetMessagesItemCounter(): Unit = addGetMessagesItemCounter(1L)
  def addGetMessagesItemCallCounter(value: Long): Unit
  def incrementGetMessagesItemCallCounter(): Unit = addGetMessagesItemCallCounter(1L)
  def addGetMessagesItemCallErrorCounter(value: Long): Unit
  def incrementGetMessagesItemCallErrorCounter(): Unit = addGetMessagesItemCallErrorCounter(1L)
  def setGetMessagesCallDuration(value: Long): Unit
  def addGetMessagesCallCounter(value: Long): Unit
  def incrementGetMessagesCallCounter(): Unit = addGetMessagesCallCounter(1L)
  def addGetMessagesCallErrorCounter(value: Long): Unit
  def incrementGetMessagesCallErrorCounter(): Unit = addGetMessagesCallErrorCounter(1L)

  def setAllPersistenceIdsItemDuration(value: Long): Unit
  def addAllPersistenceIdsItemCounter(value: Long): Unit
  def incrementAllPersistenceIdsItemCounter(): Unit = addAllPersistenceIdsItemCounter(1L)
  def addAllPersistenceIdsItemCallCounter(value: Long): Unit
  def incrementAllPersistenceIdsItemCallCounter(): Unit = addAllPersistenceIdsItemCallCounter(1L)
  def addAllPersistenceIdsItemCallErrorCounter(value: Long): Unit
  def incrementAllPersistenceIdsItemCallErrorCounter(): Unit = addAllPersistenceIdsItemCallErrorCounter(1L)
  def setAllPersistenceIdsCallDuration(value: Long): Unit
  def addAllPersistenceIdsCallCounter(value: Long): Unit
  def incrementAllPersistenceIdsCallCounter(): Unit = addAllPersistenceIdsCallCounter(1L)
  def addAllPersistenceIdsCallErrorCounter(value: Long): Unit
  def incrementAllPersistenceIdsCallErrorCounter(): Unit = addAllPersistenceIdsCallErrorCounter(1L)

  def setEventsByTagItemDuration(value: Long): Unit
  def addEventsByTagItemCounter(value: Long): Unit
  def incrementEventsByTagItemCounter(): Unit = addEventsByTagItemCounter(1L)
  def addEventsByTagItemCallCounter(value: Long): Unit
  def incrementEventsByTagItemCallCounter(): Unit = addEventsByTagItemCallCounter(1L)

  def addEventsByTagItemErrorCounter(value: Long): Unit
  def incrementEventsByTagItemCallErrorCounter(): Unit = addEventsByTagItemErrorCounter(1L)
  def setEventsByTagCallDuration(value: Long): Unit
  def addEventsByTagCallCounter(value: Long): Unit
  def incrementEventsByTagCallCounter(): Unit = addEventsByTagCallCounter(1L)
  def addEventsByTagCallErrorCounter(value: Long): Unit
  def incrementEventsByTagCallErrorCounter(): Unit = addEventsByTagCallErrorCounter(1L)

  def setJournalSequenceItemDuration(value: Long): Unit
  def addJournalSequenceItemCounter(value: Long): Unit
  def incrementJournalSequenceItemCounter(): Unit = addJournalSequenceItemCounter(1L)
  def setJournalSequenceCallDuration(value: Long): Unit
  def addJournalSequenceCallCounter(value: Long): Unit
  def incrementJournalSequenceCallCounter(): Unit = addJournalSequenceCallCounter(1L)
  def addJournalSequenceCallErrorCounter(value: Long): Unit
  def incrementJournalSequenceCallErrorCounter(): Unit = addJournalSequenceCallErrorCounter(1L)
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
  override def setAsyncWriteMessagesCallDuration(value: Long): Unit = {}

  override def addAsyncWriteMessagesCallCounter(value: Long): Unit = {}

  override def addAsyncWriteMessagesCallErrorCounter(value: Long): Unit = {}

  override def setAsyncDeleteMessagesToCallDuration(value: Long): Unit = {}

  override def addAsyncDeleteMessagesToCallCounter(value: Long): Unit = {}

  override def addAsyncDeleteMessagesToCallErrorCounter(value: Long): Unit = {}

  override def setAsyncReplayMessagesCallDuration(value: Long): Unit = {}

  override def addAsyncReplayMessagesCallCounter(value: Long): Unit = {}

  override def addAsyncReplayMessagesCallErrorCounter(value: Long): Unit = {}

  override def setAsyncReadHighestSequenceNrCallDuration(value: Long): Unit = {}

  override def addAsyncReadHighestSequenceNrCallCounter(value: Long): Unit = {}

  override def addAsyncReadHighestSequenceNrCallErrorCounter(value: Long): Unit = {}

  override def addPutMessagesEnqueueCounter(value: Long): Unit = {}

  override def addPutMessagesDequeueCounter(value: Long): Unit = {}

  override def addDeleteMessagesEnqueueCounter(value: Long): Unit = {}

  override def addDeleteMessagesDequeueCounter(value: Long): Unit = {}

  override def setPutMessagesCallDuration(value: Long): Unit = {}

  override def addPutMessagesCallCounter(value: Long): Unit = {}

  override def addPutMessagesCallErrorCounter(value: Long): Unit = {}

  override def setDeleteMessagesCallDuration(value: Long): Unit = {}

  override def addDeleteMessagesCallCounter(value: Long): Unit = {}

  override def addDeleteMessagesCallErrorCounter(value: Long): Unit = {}

  override def setGetJournalRowsItemDuration(value: Long): Unit = {}

  override def addGetJournalRowsItemCounter(value: Long): Unit = {}

  override def addGetJournalRowsItemCallCounter(value: Long): Unit = {}

  override def addGetJournalRowsItemCallErrorCounter(value: Long): Unit = {}

  override def setUpdateMessageCallDuration(value: Long): Unit = {}

  override def addUpdateMessageCallCounter(value: Long): Unit = {}

  override def addUpdateMessageCallErrorCounter(value: Long): Unit = {}

  override def setPutJournalRowsItemDuration(value: Long): Unit = {}

  override def addPutJournalRowsItemCounter(value: Long): Unit = {}

  override def addPutJournalRowsItemCallCounter(value: Long): Unit = {}

  override def addPutJournalRowsItemCallErrorCounter(value: Long): Unit = {}

  override def setPutJournalRowsCallDuration(value: Long): Unit = {}

  override def addPutJournalRowsCallCounter(value: Long): Unit = {}

  override def addPutJournalRowsCallErrorCounter(value: Long): Unit = {}

  override def setDeleteJournalRowsItemDuration(value: Long): Unit = {}

  override def addDeleteJournalRowsItemCounter(value: Long): Unit = {}

  override def addDeleteJournalRowsItemCallCounter(value: Long): Unit = {}

  override def addDeleteJournalRowsItemCallErrorCounter(value: Long): Unit = {}

  override def setDeleteJournalRowsCallDuration(value: Long): Unit = {}

  override def addDeleteJournalRowsCallCounter(value: Long): Unit = {}

  override def addDeleteJournalRowsCallErrorCounter(value: Long): Unit = {}

  override def setHighestSequenceNrItemDuration(value: Long): Unit = {}

  override def addHighestSequenceNrItemCounter(value: Long): Unit = {}

  override def addHighestSequenceNrItemCallCounter(value: Long): Unit = {}

  override def addHighestSequenceNrItemCallErrorCounter(value: Long): Unit = {}

  override def setHighestSequenceNrCallDuration(value: Long): Unit = {}

  override def addHighestSequenceNrCallCounter(value: Long): Unit = {}

  override def setHighestSequenceNrCallErrorCounter(value: Long): Unit = {}

  override def setGetMessagesItemDuration(value: Long): Unit = {}

  override def addGetMessagesItemCounter(value: Long): Unit = {}

  override def addGetMessagesItemCallCounter(value: Long): Unit = {}

  override def addGetMessagesItemCallErrorCounter(value: Long): Unit = {}

  override def setGetMessagesCallDuration(value: Long): Unit = {}

  override def addGetMessagesCallCounter(value: Long): Unit = {}

  override def addGetMessagesCallErrorCounter(value: Long): Unit = {}

  override def setAllPersistenceIdsItemDuration(value: Long): Unit = {}

  override def addAllPersistenceIdsItemCounter(value: Long): Unit = {}

  override def addAllPersistenceIdsItemCallCounter(value: Long): Unit = {}

  override def addAllPersistenceIdsItemCallErrorCounter(value: Long): Unit = {}

  override def setAllPersistenceIdsCallDuration(value: Long): Unit = {}

  override def addAllPersistenceIdsCallCounter(value: Long): Unit = {}

  override def addAllPersistenceIdsCallErrorCounter(value: Long): Unit = {}

  override def setEventsByTagItemDuration(value: Long): Unit = {}

  override def addEventsByTagItemCounter(value: Long): Unit = {}

  override def addEventsByTagItemCallCounter(value: Long): Unit = {}

  override def addEventsByTagItemErrorCounter(value: Long): Unit = {}

  override def setEventsByTagCallDuration(value: Long): Unit = {}

  override def addEventsByTagCallCounter(value: Long): Unit = {}

  override def addEventsByTagCallErrorCounter(value: Long): Unit = {}

  override def setJournalSequenceItemDuration(value: Long): Unit = {}

  override def addJournalSequenceItemCounter(value: Long): Unit = {}

  override def setJournalSequenceCallDuration(value: Long): Unit = {}

  override def addJournalSequenceCallCounter(value: Long): Unit = {}

  override def addJournalSequenceCallErrorCounter(value: Long): Unit = {}
}
