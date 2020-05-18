package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe

trait MetricsReporter {
  def setPutItemDuration(duration: Duration): Unit
  def setBatchWriteItemDuration(duration: Duration): Unit
  def setUpdateItemDuration(duration: Duration): Unit
  def setDeleteItemDuration(duration: Duration): Unit
  def setQueryDuration(duration: Duration): Unit
  def setScanDuration(duration: Duration): Unit
}

object MetricsReporter {

  def create(className: String): MetricsReporter = {
    val runtimeMirror     = universe.runtimeMirror(getClass.getClassLoader)
    val classSymbol       = runtimeMirror.staticClass(className)
    val classMirror       = runtimeMirror.reflectClass(classSymbol)
    val constructorMethod = classSymbol.typeSignature.decl(universe.termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructorMethod)
    val newInstance       = constructorMirror()
    newInstance.asInstanceOf[MetricsReporter]
  }

}

class NullMetricsReporter extends MetricsReporter {
  override def setPutItemDuration(duration: Duration): Unit        = {}
  override def setBatchWriteItemDuration(duration: Duration): Unit = {}
  override def setUpdateItemDuration(duration: Duration): Unit     = {}
  override def setDeleteItemDuration(duration: Duration): Unit     = {}
  override def setQueryDuration(duration: Duration): Unit          = {}
  override def setScanDuration(duration: Duration): Unit           = {}

}
