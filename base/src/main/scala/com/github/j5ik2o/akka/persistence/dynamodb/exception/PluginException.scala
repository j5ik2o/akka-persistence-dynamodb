package com.github.j5ik2o.akka.persistence.dynamodb.exception

final class PluginException(message: String, cause: Option[Throwable] = None) extends Exception(message, cause.orNull)
