package com.github.j5ik2o.akka.persistence.dynamodb.journal

final case class JournalRow(
    persistenceId: PersistenceId,
    sequenceNumber: SequenceNumber,
    deleted: Boolean,
    message: Array[Byte],
    ordering: Long,
    tags: Option[String] = None
) {
  def partitionKey: PartitionKey            = PartitionKey(persistenceId, sequenceNumber)
  def withDeleted: JournalRow               = copy(deleted = true)
  def withOrdering(value: Long): JournalRow = copy(ordering = value)
}
