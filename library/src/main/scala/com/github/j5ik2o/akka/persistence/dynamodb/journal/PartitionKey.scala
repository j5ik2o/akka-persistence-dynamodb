package com.github.j5ik2o.akka.persistence.dynamodb.journal

case class PartitionKey(persistenceId: PersistenceId, sequenceNumber: SequenceNumber) {

  def asString(shardCount: Int) =
    s"${persistenceId.asString}-${sequenceNumber.value % shardCount}"

}
