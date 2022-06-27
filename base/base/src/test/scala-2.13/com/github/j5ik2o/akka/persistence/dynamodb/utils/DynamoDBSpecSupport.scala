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
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import org.scalatest.{ BeforeAndAfter, Suite }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.slf4j.bridge.SLF4JBridgeHandler
import org.slf4j.{ Logger, LoggerFactory }

trait DynamoDBSpecSupport
    extends Matchers
    with Eventually
    with BeforeAndAfter
    with ScalaFutures
    with DynamoDBContainerSpecSupport {
  this: Suite =>

  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

  val logger: Logger = LoggerFactory.getLogger(getClass)

}
