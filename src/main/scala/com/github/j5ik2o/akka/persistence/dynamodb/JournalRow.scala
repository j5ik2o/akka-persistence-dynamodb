package com.github.j5ik2o.akka.persistence.dynamodb

final case class JournalRow(persistenceId: String,
                            sequenceNumber: Long,
                            deleted: Boolean,
                            message: Array[Byte],
                            ordering: Long,
                            tags: Option[String] = None) {
  def withDeleted: JournalRow               = copy(deleted = true)
  def withOrdering(value: Long): JournalRow = copy(ordering = value)
}
