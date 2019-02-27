package com.github.j5ik2o.akka.persistence.dynamodb.journal

final case class JournalRow(ordering: Long,
                            deleted: Boolean,
                            persistenceId: String,
                            sequenceNumber: Long,
                            message: Array[Byte],
                            tags: Option[String] = None) {
  def withDeleted: JournalRow = copy(deleted = true)
}
