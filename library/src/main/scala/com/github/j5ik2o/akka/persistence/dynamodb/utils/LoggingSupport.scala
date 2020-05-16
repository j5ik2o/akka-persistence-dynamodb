package com.github.j5ik2o.akka.persistence.dynamodb.utils

import org.slf4j.LoggerFactory

trait LoggingSupport {

  protected val logger = LoggerFactory.getLogger(getClass)

}
