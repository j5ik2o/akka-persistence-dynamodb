package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao

case class SnapshotRow(persistenceId: String, sequenceNumber: Long, created: Long, snapshot: Array[Byte])
