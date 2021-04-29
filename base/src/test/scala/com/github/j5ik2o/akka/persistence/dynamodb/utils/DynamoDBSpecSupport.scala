package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.github.j5ik2o.dockerController.{
  DockerContainerCreateRemoveLifecycle,
  DockerContainerStartStopLifecycle,
  DockerControllerSpecSupport
}
import org.scalatest.TestSuite
import org.slf4j.bridge.SLF4JBridgeHandler

trait DynamoDBSpecSupport extends DynamoDBContainerHelper with DockerControllerSpecSupport {
  this: TestSuite =>

  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

  override protected def createRemoveLifecycle: DockerContainerCreateRemoveLifecycle.Value =
    DockerContainerCreateRemoveLifecycle.ForAllTest

  override protected def startStopLifecycle: DockerContainerStartStopLifecycle.Value =
    DockerContainerStartStopLifecycle.ForAllTest
}
