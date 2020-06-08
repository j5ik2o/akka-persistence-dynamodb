package com.github.j5ik2o.akka.persistence.dynamodb.journal

final class PersistenceIdOps(self: PersistenceId, separator: String) {
  private def value                       = self.asString
  def hasHyphen: Boolean                  = value.contains(separator)
  private def splits: Option[Seq[String]] = if (hasHyphen) Some(value.split(separator)) else None
  def prefix: Option[String]              = splits.map(_(0))
  def body: Option[String]                = splits.map(_(1))
}

trait ToPersistenceIdOps {

  def separator: String

  implicit def ToPersistenceIdOps(self: PersistenceId): PersistenceIdOps = new PersistenceIdOps(self, separator)

}
