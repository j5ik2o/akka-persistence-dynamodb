/*
 * Copyright 2017 Dennis Vriend
 * Copyright 2019 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.query

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import akka.persistence.query.javadsl.{ ReadJournal => JavaReadJournal }
import akka.persistence.query.scaladsl.{ ReadJournal => ScalaReadJournal }
import com.github.j5ik2o.akka.persistence.dynamodb.query.javadsl.{ DynamoDBReadJournal => JavaDynamoDBReadJournal }
import com.github.j5ik2o.akka.persistence.dynamodb.query.scaladsl.{ DynamoDBReadJournal => ScalaDynamoDBReadJournal }
import com.typesafe.config.Config

class DynamoDBReadJournalProvider(system: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {
  private val scalaReadJournal = new ScalaDynamoDBReadJournal(config, configPath)(system)
  private val javaReadJournal  = new JavaDynamoDBReadJournal(scalaReadJournal)

  override def scaladslReadJournal(): ScalaReadJournal = scalaReadJournal
  override def javadslReadJournal(): JavaReadJournal   = javaReadJournal
}
