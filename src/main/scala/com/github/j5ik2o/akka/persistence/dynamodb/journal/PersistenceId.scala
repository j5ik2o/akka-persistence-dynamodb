package com.github.j5ik2o.akka.persistence.dynamodb.journal

case class PersistenceId(value: String) {
  require(value.length >= 1 && value.length <= 2048)
  def asString: String = value
}
