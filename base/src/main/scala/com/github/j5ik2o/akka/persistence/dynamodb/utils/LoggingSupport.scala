package com.github.j5ik2o.akka.persistence.dynamodb.utils

import org.slf4j.{ Logger, LoggerFactory }

trait LoggingSupport {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

}
