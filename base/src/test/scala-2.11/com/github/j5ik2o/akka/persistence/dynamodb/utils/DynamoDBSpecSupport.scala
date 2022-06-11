package com.github.j5ik2o.akka.persistence.dynamodb.utils

import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

trait DynamoDBSpecSupport
    extends Matchers
    with Eventually
    with BeforeAndAfter
    with ScalaFutures
    with DynamoDBContainerSpecSupport {
  this: Suite =>
  private implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

  val logger: Logger = LoggerFactory.getLogger(getClass)

}
