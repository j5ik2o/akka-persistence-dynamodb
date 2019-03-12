package com.github.j5ik2o.akka.persistence.dynamodb.journal

object SequenceNumber {
  val MaxValue = SequenceNumber(Long.MaxValue)
  val MinValue = SequenceNumber(0L)
}

case class SequenceNumber(value: Long) extends Ordered[SequenceNumber] {
  override def compare(that: SequenceNumber): Int = {
    value compare that.value
  }
  def asString: String = value.toString
}
