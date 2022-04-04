package com.github.j5ik2o.akka.persistence.dynamodb.trace

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.model.Context

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.{ Failure, Success }

trait TraceReporter {

  def traceJournalAsyncWriteMessages[T](context: Context)(f: => Future[T]): Future[T]

}

object TraceReporter {

  class None(pluginConfig: PluginConfig) extends TraceReporter {
    def traceJournalAsyncWriteMessages[T](context: Context)(f: => Future[T]): Future[T] = f
  }

}

trait TraceReporterProvider {

  def create: Option[TraceReporter]

}

object TraceReporterProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): TraceReporterProvider = {
    val className = pluginConfig.traceReporterProviderClassName
    dynamicAccess
      .createInstanceFor[TraceReporterProvider](
        className,
        Seq(classOf[DynamicAccess] -> dynamicAccess, classOf[PluginConfig] -> pluginConfig)
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize TraceReporterProvider", Some(ex))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends TraceReporterProvider {

    def create: Option[TraceReporter] = {
      pluginConfig.traceReporterClassName.map { className =>
        dynamicAccess
          .createInstanceFor[TraceReporter](
            className,
            Seq(classOf[PluginConfig] -> pluginConfig)
          ) match {
          case Success(value) => value
          case Failure(ex) =>
            throw new PluginException("Failed to initialize TraceReporter", Some(ex))
        }
      }
    }

  }
}
