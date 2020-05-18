package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import com.amazonaws.retry.{ PredefinedRetryPolicies, RetryPolicy }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

trait V1RetryPolicyProvider {
  def create: RetryPolicy
}

object V1RetryPolicyProvider {

  class Default(dynamoDBClientConfig: DynamoDBClientConfig) extends V1RetryPolicyProvider {

    override def create: RetryPolicy = {
      dynamoDBClientConfig.v1ClientConfig.maxErrorRetry.fold(PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicy) {
        maxErrorRetry => PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(maxErrorRetry)
      }
    }

  }

}
