package com.github.j5ik2o.akka.persistence.dynamodb.model

final class PersistenceId(private val value: String) {
  require(value.length >= 1 && value.length <= 2048, "Invalid string length")

  override def toString: String = s"PersistenceId($value)"

  def asString: String = value

  override def equals(other: Any): Boolean = other match {
    case that: PersistenceId =>
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object PersistenceId {
  val Separator = "-"

  def apply(value: String): PersistenceId = new PersistenceId(value)
}
