package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.dimafeng.testcontainers.{ FixedHostPortGenericContainer, ForAllTestContainer }
import org.scalatest.{ BeforeAndAfterAll, Suite }

trait DynamoDBContainerSpecSupport extends DynamoDBContainerHelper with ForAllTestContainer with BeforeAndAfterAll {
  this: Suite =>

  override val container: FixedHostPortGenericContainer = dynamoDbLocalContainer

}
