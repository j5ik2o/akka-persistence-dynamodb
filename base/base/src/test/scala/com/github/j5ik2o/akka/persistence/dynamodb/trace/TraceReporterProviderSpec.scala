/*
 * Copyright 2022 Junichi Kato
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

import akka.actor.{ ActorSystem, DynamicAccess }
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.dynamodb.PluginTestUtils
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import org.scalatest.freespec.AnyFreeSpecLike

class TR_None() extends TraceReporter
class TR_PC(pluginConfig: PluginConfig) extends TraceReporter
class TR_DA(dynamicAccess: DynamicAccess) extends TraceReporter
class TR_DA_PC(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends TraceReporter
class TR_CTX(pluginContext: PluginContext) extends TraceReporter

class TraceReporterProviderSpec extends TestKit(ActorSystem("TraceReporterProvider")) with AnyFreeSpecLike {
  "TraceReporterProvider" - {
    "create None" in {
      val p = TraceReporterProvider.create(PluginTestUtils.newPluginContext(system, None, Some(classOf[TR_None])))
      val r = p.create
      assert(r.isDefined)
    }
    "create PC" in {
      val p = TraceReporterProvider.create(PluginTestUtils.newPluginContext(system, None, Some(classOf[TR_PC])))
      val r = p.create
      assert(r.isDefined)
    }
    "create DA" in {
      val p = TraceReporterProvider.create(PluginTestUtils.newPluginContext(system, None, Some(classOf[TR_DA])))
      val r = p.create
      assert(r.isDefined)
    }
    "create DA PC" in {
      val p = TraceReporterProvider.create(PluginTestUtils.newPluginContext(system, None, Some(classOf[TR_DA_PC])))
      val r = p.create
      assert(r.isDefined)
    }
    "create CTX" in {
      val p = TraceReporterProvider.create(PluginTestUtils.newPluginContext(system, None, Some(classOf[TR_CTX])))
      val r = p.create
      assert(r.isDefined)
    }
  }

}
