package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import software.amazon.awssdk.core.retry.RetryPolicy

trait V2RetryPolicyProvider {
  def create: RetryPolicy
}

object V2RetryPolicyProvider {

  class Default(dynamoDBClientConfig: DynamoDBClientConfig) extends V2RetryPolicyProvider {

    override def create: RetryPolicy = {
      RetryPolicy.defaultRetryPolicy()
    }
  }

}
