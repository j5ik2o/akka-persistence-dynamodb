package com.github.j5ik2o.akka.persistence.dynamodb.exception

class PluginException(message: String, cause: Option[Throwable] = None) extends Exception(message, cause.orNull)
