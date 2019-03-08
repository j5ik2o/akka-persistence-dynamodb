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
