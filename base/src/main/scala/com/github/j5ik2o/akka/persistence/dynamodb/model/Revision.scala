package com.github.j5ik2o.akka.persistence.dynamodb.model

object Revision {
  val MaxValue: Revision = Revision(Long.MaxValue)
  val MinValue: Revision = Revision(0L)
}

case class Revision(value: Long) extends Ordered[Revision] {
  require(value >= 0, "Invalid value")

  override def toString: String = s"Revision($value)"

  override def compare(that: Revision): Int = {
    value compare that.value
  }

  def asString: String = value.toString
}
