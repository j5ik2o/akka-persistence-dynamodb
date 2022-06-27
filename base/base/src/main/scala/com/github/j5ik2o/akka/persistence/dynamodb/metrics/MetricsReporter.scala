/*
 * Copyright 2021 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.model.Context
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessUtils

import scala.annotation.unused

trait MetricsReporter {

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

  final class None() extends MetricsReporter

}

trait MetricsReporterProvider {

  def create: Option[MetricsReporter]

}

object MetricsReporterProvider {

  def create(pluginContext: PluginContext): MetricsReporterProvider = {
    val className = pluginContext.pluginConfig.metricsReporterProviderClassName
    DynamicAccessUtils
      .createInstanceFor_CTX_Throw[MetricsReporterProvider, PluginContext](
        className,
        pluginContext
      )
  }

  final class Default(pluginContext: PluginContext) extends MetricsReporterProvider {

    def create: Option[MetricsReporter] = {
      import pluginContext._
      pluginConfig.metricsReporterClassName.map { className =>
        DynamicAccessUtils
          .createInstanceFor_CTX_Throw[MetricsReporter, PluginContext](className, pluginContext)
      }
    }

  }
}
