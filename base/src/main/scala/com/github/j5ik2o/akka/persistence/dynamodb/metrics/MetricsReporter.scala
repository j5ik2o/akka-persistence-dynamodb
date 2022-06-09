package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.model.Context

import scala.annotation.unused
import scala.collection.immutable._
import scala.util.{ Failure, Success }

/** MetricsReporter.
  */
abstract class MetricsReporter(val pluginConfig: PluginConfig) {

  def beforeJournalAsyncWriteMessages(context: Context): Context                            = { context }
  def afterJournalAsyncWriteMessages(@unused context: Context): Unit                        = {}
  def errorJournalAsyncWriteMessages(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeJournalAsyncDeleteMessagesTo(@unused context: Context): Context                    = { context }
  def afterJournalAsyncDeleteMessagesTo(@unused context: Context): Unit                        = {}
  def errorJournalAsyncDeleteMessagesTo(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeJournalAsyncReplayMessages(context: Context): Context                            = { context }
  def afterJournalAsyncReplayMessages(@unused context: Context): Unit                        = {}
  def errorJournalAsyncReplayMessages(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeJournalAsyncReadHighestSequenceNr(context: Context): Context                            = { context }
  def afterJournalAsyncReadHighestSequenceNr(@unused context: Context): Unit                        = {}
  def errorJournalAsyncReadHighestSequenceNr(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeJournalAsyncUpdateEvent(context: Context): Context                            = { context }
  def afterJournalAsyncUpdateEvent(@unused context: Context): Unit                        = {}
  def errorJournalAsyncUpdateEvent(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeJournalSerializeJournal(context: Context): Context                            = { context }
  def afterJournalSerializeJournal(@unused context: Context): Unit                        = {}
  def errorJournalSerializeJournal(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeJournalDeserializeJournal(context: Context): Context                            = { context }
  def afterJournalDeserializeJournal(@unused context: Context): Unit                        = {}
  def errorJournalDeserializeJournal(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeSnapshotStoreLoadAsync(context: Context): Context                            = { context }
  def afterSnapshotStoreLoadAsync(@unused context: Context): Unit                        = {}
  def errorSnapshotStoreLoadAsync(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeSnapshotStoreSaveAsync(context: Context): Context                            = { context }
  def afterSnapshotStoreSaveAsync(@unused context: Context): Unit                        = {}
  def errorSnapshotStoreSaveAsync(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeSnapshotStoreDeleteAsync(context: Context): Context                            = { context }
  def afterSnapshotStoreDeleteAsync(@unused context: Context): Unit                        = {}
  def errorSnapshotStoreDeleteAsync(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeSnapshotStoreDeleteWithCriteriaAsync(context: Context): Context                            = { context }
  def afterSnapshotStoreDeleteWithCriteriaAsync(@unused context: Context): Unit                        = {}
  def errorSnapshotStoreDeleteWithCriteriaAsync(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeSnapshotStoreSerializeSnapshot(context: Context): Context                            = { context }
  def afterSnapshotStoreSerializeSnapshot(@unused context: Context): Unit                        = {}
  def errorSnapshotStoreSerializeSnapshot(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeSnapshotStoreDeserializeSnapshot(context: Context): Context                            = { context }
  def afterSnapshotStoreDeserializeSnapshot(@unused context: Context): Unit                        = {}
  def errorSnapshotStoreDeserializeSnapshot(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeStateStoreUpsertObject(context: Context): Context                            = { context }
  def afterStateStoreUpsertObject(@unused context: Context): Unit                        = {}
  def errorStateStoreUpsertObject(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeStateStoreGetObject(context: Context): Context                            = { context }
  def afterStateStoreGetObject(@unused context: Context): Unit                        = {}
  def errorStateStoreGetObject(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeStateStoreDeleteObject(context: Context): Context                            = { context }
  def afterStateStoreDeleteObject(@unused context: Context): Unit                        = {}
  def errorStateStoreDeleteObject(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeStateStoreSerializeState(context: Context): Context                            = { context }
  def afterStateStoreSerializeState(@unused context: Context): Unit                        = {}
  def errorStateStoreSerializeState(@unused context: Context, @unused ex: Throwable): Unit = {}

  def beforeStateStoreDeserializeState(context: Context): Context                            = { context }
  def afterStateStoreDeserializeState(@unused context: Context): Unit                        = {}
  def errorStateStoreDeserializeState(@unused context: Context, @unused ex: Throwable): Unit = {}
}

object MetricsReporter {

  final class None(pluginConfig: PluginConfig) extends MetricsReporter(pluginConfig)

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
