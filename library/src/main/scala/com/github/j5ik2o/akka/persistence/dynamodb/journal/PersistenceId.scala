package com.github.j5ik2o.akka.persistence.dynamodb.journal

final case class PersistenceId(private val value: String) {
  require(value.length >= 1 && value.length <= 2048, "Invalid string length")
  // require(value.contains("-"), "There are no hyphens in the string")

  def asString: String = value

  lazy val hasHyphen: Boolean             = value.contains("-")
  private val splits: Option[Seq[String]] = if (hasHyphen) Some(value.split("-")) else None
  lazy val modelName: Option[String]      = splits.map(_(0))
  lazy val id: Option[String]             = splits.map(_(1))
}
