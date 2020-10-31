package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException

import scala.concurrent.duration.Duration
import scala.collection.immutable._
import scala.util.{ Failure, Success, Try }

trait MetricsReporter {

  def beforeJournalAsyncWriteMessages(): Unit
  def beforeJournalAsyncDeleteMessagesTo(): Unit
  def beforeJournalAsyncReplayMessages(): Unit
  def beforeJournalAsyncReadHighestSequenceNr(): Unit
  def beforeJournalAsyncUpdateEvent(): Unit

  def afterJournalAsyncWriteMessages(): Unit
  def afterJournalAsyncDeleteMessagesTo(): Unit
  def afterJournalAsyncReplayMessages(): Unit
  def afterJournalAsyncReadHighestSequenceNr(): Unit
  def afterJournalAsyncUpdateEvent(): Unit

  def errorJournalAsyncWriteMessages(ex: Throwable): Unit
  def errorJournalAsyncDeleteMessagesTo(ex: Throwable): Unit
  def errorJournalAsyncReplayMessages(ex: Throwable): Unit
  def errorJournalAsyncReadHighestSequenceNr(ex: Throwable): Unit
  def errorJournalAsyncUpdateEvent(ex: Throwable): Unit

  def beforeSnapshotStoreLoadAsync(): Unit
  def afterSnapshotStoreLoadAsync(): Unit
  def errorSnapshotStoreLoadAsync(ex: Throwable): Unit

  def beforeSnapshotStoreSaveAsync(): Unit
  def afterSnapshotStoreSaveAsync(): Unit
  def errorSnapshotStoreSaveAsync(ex: Throwable): Unit

  def beforeSnapshotStoreDeleteAsync(): Unit
  def afterSnapshotStoreDeleteAsync(): Unit
  def errorSnapshotStoreDeleteAsync(ex: Throwable): Unit
}

object MetricsReporter {

  class None(pluginConfig: PluginConfig) extends MetricsReporter {
    override def beforeJournalAsyncWriteMessages(): Unit         = {}
    override def beforeJournalAsyncDeleteMessagesTo(): Unit      = {}
    override def beforeJournalAsyncReplayMessages(): Unit        = {}
    override def beforeJournalAsyncReadHighestSequenceNr(): Unit = {}
    override def beforeJournalAsyncUpdateEvent(): Unit           = {}

    override def afterJournalAsyncWriteMessages(): Unit         = {}
    override def afterJournalAsyncDeleteMessagesTo(): Unit      = {}
    override def afterJournalAsyncReplayMessages(): Unit        = {}
    override def afterJournalAsyncReadHighestSequenceNr(): Unit = {}
    override def afterJournalAsyncUpdateEvent(): Unit           = {}

    override def errorJournalAsyncWriteMessages(ex: Throwable): Unit         = {}
    override def errorJournalAsyncDeleteMessagesTo(ex: Throwable): Unit      = {}
    override def errorJournalAsyncReplayMessages(ex: Throwable): Unit        = {}
    override def errorJournalAsyncReadHighestSequenceNr(ex: Throwable): Unit = {}
    override def errorJournalAsyncUpdateEvent(ex: Throwable): Unit           = {}

    override def beforeSnapshotStoreLoadAsync(): Unit   = {}
    override def beforeSnapshotStoreSaveAsync(): Unit   = {}
    override def beforeSnapshotStoreDeleteAsync(): Unit = {}

    override def afterSnapshotStoreLoadAsync(): Unit   = {}
    override def afterSnapshotStoreSaveAsync(): Unit   = {}
    override def afterSnapshotStoreDeleteAsync(): Unit = {}

    override def errorSnapshotStoreLoadAsync(ex: Throwable): Unit   = {}
    override def errorSnapshotStoreSaveAsync(ex: Throwable): Unit   = {}
    override def errorSnapshotStoreDeleteAsync(ex: Throwable): Unit = {}
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
