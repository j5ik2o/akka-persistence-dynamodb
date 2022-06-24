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
package com.github.j5ik2o.akka.persistence.dynamodb.trace

import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.model.Context
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessUtils

import scala.annotation.unused
import scala.concurrent.Future

trait TraceReporter {

  def traceJournalAsyncWriteMessages[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceJournalAsyncDeleteMessagesTo[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceJournalAsyncReplayMessages[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceJournalAsyncReadHighestSequenceNr[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceJournalAsyncUpdateEvent[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceJournalSerializeJournal[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceJournalDeserializeJournal[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceSnapshotStoreLoadAsync[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceSnapshotStoreSaveAsync[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceSnapshotStoreDeleteAsync[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceSnapshotStoreDeleteWithCriteriaAsync[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceSnapshotStoreSerializeSnapshot[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceSnapshotStoreDeserializeSnapshot[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceStateStoreGetObject[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceStateStoreUpsertObject[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceStateStoreDeleteObject[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceStateStoreSerializeState[T](@unused context: Context)(f: => Future[T]): Future[T] = f

  def traceStateStoreDeserializeState[T](@unused context: Context)(f: => Future[T]): Future[T] = f

}

object TraceReporter {

  final class None extends TraceReporter

}

trait TraceReporterProvider {

  def create: Option[TraceReporter]

}

object TraceReporterProvider {

  def create(pluginContext: PluginContext): TraceReporterProvider = {
    val className = pluginContext.pluginConfig.traceReporterProviderClassName
    DynamicAccessUtils.createInstanceFor_CTX_Throw[TraceReporterProvider, PluginContext](
      className,
      pluginContext
    )
  }

  final class Default(pluginContext: PluginContext) extends TraceReporterProvider {

    def create: Option[TraceReporter] = {
      pluginContext.pluginConfig.traceReporterClassName.map { className =>
        DynamicAccessUtils.createInstanceFor_CTX_Throw[TraceReporter, PluginContext](
          className,
          pluginContext
        )
      }
    }

  }
}
