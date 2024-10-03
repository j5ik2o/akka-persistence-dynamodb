package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

final class RevisionMismatchException(message: String, cause: Option[Throwable] = None) extends Exception(message, cause.orNull)
