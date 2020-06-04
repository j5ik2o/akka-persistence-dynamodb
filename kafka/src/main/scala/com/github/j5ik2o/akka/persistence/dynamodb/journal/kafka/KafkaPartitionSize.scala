package com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka

case class KafkaPartitionSize(value: Int) {
  require(value > 0)
}
