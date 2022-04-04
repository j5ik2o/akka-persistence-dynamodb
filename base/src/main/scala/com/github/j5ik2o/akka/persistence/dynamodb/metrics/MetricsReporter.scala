package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.model.Context

import scala.collection.immutable._
import scala.util.{ Failure, Success }

trait MetricsReporter {

  def beforeJournalAsyncWriteMessages(context: Context): Context = { context }
  def afterJournalAsyncWriteMessages(context: Context): Unit = {}
  def errorJournalAsyncWriteMessages(context: Context, ex: Throwable): Unit = {}

  def beforeJournalAsyncDeleteMessagesTo(context: Context): Context = { context }
  def afterJournalAsyncDeleteMessagesTo(context: Context): Unit = {}
  def errorJournalAsyncDeleteMessagesTo(context: Context, ex: Throwable): Unit = {}

  def beforeJournalAsyncReplayMessages(context: Context): Context = { context }
  def afterJournalAsyncReplayMessages(context: Context): Unit = {}
  def errorJournalAsyncReplayMessages(context: Context, ex: Throwable): Unit = {}

  def beforeJournalAsyncReadHighestSequenceNr(context: Context): Context = { context }
  def afterJournalAsyncReadHighestSequenceNr(context: Context): Unit = {}
  def errorJournalAsyncReadHighestSequenceNr(context: Context, ex: Throwable): Unit = {}

  def beforeJournalAsyncUpdateEvent(context: Context): Context = { context }
  def afterJournalAsyncUpdateEvent(context: Context): Unit = {}
  def errorJournalAsyncUpdateEvent(context: Context, ex: Throwable): Unit = {}

  def beforeJournalSerializeJournal(context: Context): Context = { context }
  def afterJournalSerializeJournal(context: Context): Unit = {}
  def errorJournalSerializeJournal(context: Context, ex: Throwable): Unit = {}

  def beforeJournalDeserializeJournal(context: Context): Context = { context }
  def afterJournalDeserializeJournal(context: Context): Unit = {}
  def errorJournalDeserializeJournal(context: Context, ex: Throwable): Unit = {}

  def beforeSnapshotStoreLoadAsync(context: Context): Context = { context }
  def afterSnapshotStoreLoadAsync(context: Context): Unit = {}
  def errorSnapshotStoreLoadAsync(context: Context, ex: Throwable): Unit = {}

  def beforeSnapshotStoreSaveAsync(context: Context): Context = { context }
  def afterSnapshotStoreSaveAsync(context: Context): Unit = {}
  def errorSnapshotStoreSaveAsync(context: Context, ex: Throwable): Unit = {}

  def beforeSnapshotStoreDeleteAsync(context: Context): Context = { context }
  def afterSnapshotStoreDeleteAsync(context: Context): Unit = {}
  def errorSnapshotStoreDeleteAsync(context: Context, ex: Throwable): Unit = {}

  def beforeSnapshotStoreDeleteWithCriteriaAsync(context: Context): Context = { context }
  def afterSnapshotStoreDeleteWithCriteriaAsync(context: Context): Unit = {}
  def errorSnapshotStoreDeleteWithCriteriaAsync(context: Context, ex: Throwable): Unit = {}

  def beforeSnapshotStoreSerializeSnapshot(context: Context): Context = { context }
  def afterSnapshotStoreSerializeSnapshot(context: Context): Unit = {}
  def errorSnapshotStoreSerializeSnapshot(context: Context, ex: Throwable): Unit = {}

  def beforeSnapshotStoreDeserializeSnapshot(context: Context): Context = { context }
  def afterSnapshotStoreDeserializeSnapshot(context: Context): Unit = {}
  def errorSnapshotStoreDeserializeSnapshot(context: Context, ex: Throwable): Unit = {}
}

object MetricsReporter {

  class None(pluginConfig: PluginConfig) extends MetricsReporter {
    override def beforeJournalAsyncWriteMessages(context: Context): Context = { context }
    override def afterJournalAsyncWriteMessages(context: Context): Unit = {}
    override def errorJournalAsyncWriteMessages(context: Context, ex: Throwable): Unit = {}

    override def beforeJournalAsyncDeleteMessagesTo(context: Context): Context = { context }
    override def afterJournalAsyncDeleteMessagesTo(context: Context): Unit = {}
    override def errorJournalAsyncDeleteMessagesTo(context: Context, ex: Throwable): Unit = {}

    override def beforeJournalAsyncReplayMessages(context: Context): Context = { context }
    override def afterJournalAsyncReplayMessages(context: Context): Unit = {}
    override def errorJournalAsyncReplayMessages(context: Context, ex: Throwable): Unit = {}

    override def beforeJournalAsyncReadHighestSequenceNr(context: Context): Context = { context }
    override def afterJournalAsyncReadHighestSequenceNr(context: Context): Unit = {}
    override def errorJournalAsyncReadHighestSequenceNr(context: Context, ex: Throwable): Unit = {}

    override def beforeJournalAsyncUpdateEvent(context: Context): Context = { context }
    override def afterJournalAsyncUpdateEvent(context: Context): Unit = {}
    override def errorJournalAsyncUpdateEvent(context: Context, ex: Throwable): Unit = {}

    override def beforeJournalSerializeJournal(context: Context): Context = { context }
    override def afterJournalSerializeJournal(context: Context): Unit = {}
    override def errorJournalSerializeJournal(context: Context, ex: Throwable): Unit = {}

    override def beforeJournalDeserializeJournal(context: Context): Context = { context }
    override def afterJournalDeserializeJournal(context: Context): Unit = {}
    override def errorJournalDeserializeJournal(context: Context, ex: Throwable): Unit = {}

    override def beforeSnapshotStoreLoadAsync(context: Context): Context = { context }
    override def afterSnapshotStoreLoadAsync(context: Context): Unit = {}
    override def errorSnapshotStoreLoadAsync(context: Context, ex: Throwable): Unit = {}

    override def beforeSnapshotStoreSaveAsync(context: Context): Context = { context }
    override def afterSnapshotStoreSaveAsync(context: Context): Unit = {}
    override def errorSnapshotStoreSaveAsync(context: Context, ex: Throwable): Unit = {}

    override def beforeSnapshotStoreDeleteAsync(context: Context): Context = { context }
    override def afterSnapshotStoreDeleteAsync(context: Context): Unit = {}
    override def errorSnapshotStoreDeleteAsync(context: Context, ex: Throwable): Unit = {}

    override def beforeSnapshotStoreSerializeSnapshot(context: Context): Context = { context }
    override def afterSnapshotStoreSerializeSnapshot(context: Context): Unit = {}
    override def errorSnapshotStoreSerializeSnapshot(context: Context, ex: Throwable): Unit = {}

    override def beforeSnapshotStoreDeserializeSnapshot(context: Context): Context = { context }
    override def afterSnapshotStoreDeserializeSnapshot(context: Context): Unit = {}
    override def errorSnapshotStoreDeserializeSnapshot(context: Context, ex: Throwable): Unit = {}
  }

}

trait MetricsReporterProvider {

  def create: Option[MetricsReporter]

}

object MetricsReporterProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): MetricsReporterProvider = {
    val className = pluginConfig.metricsReporterProviderClassName
    dynamicAccess
      .createInstanceFor[MetricsReporterProvider](
        className,
        Seq(classOf[DynamicAccess] -> dynamicAccess, classOf[PluginConfig] -> pluginConfig)
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize MetricsReporterProvider", Some(ex))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends MetricsReporterProvider {

    def create: Option[MetricsReporter] = {
      pluginConfig.metricsReporterClassName.map { className =>
        dynamicAccess
          .createInstanceFor[MetricsReporter](
            className,
            Seq(classOf[PluginConfig] -> pluginConfig)
          ) match {
          case Success(value) => value
          case Failure(ex) =>
            throw new PluginException("Failed to initialize MetricsReporter", Some(ex))
        }
      }
    }

  }
}
