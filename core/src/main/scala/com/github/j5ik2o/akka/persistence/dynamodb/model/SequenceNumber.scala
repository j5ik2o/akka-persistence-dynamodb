package com.github.j5ik2o.akka.persistence.dynamodb.model

object SequenceNumber {
  val MaxValue = SequenceNumber(Long.MaxValue)
  val MinValue = SequenceNumber(0L)
}

case class SequenceNumber(value: Long) extends Ordered[SequenceNumber] {
  require(value >= 0, "Invalid value")

  override def toString: String = s"SequenceNumber($value)"

  override def compare(that: SequenceNumber): Int = {
    value compare that.value
  }

  def asString: String = value.toString
}
