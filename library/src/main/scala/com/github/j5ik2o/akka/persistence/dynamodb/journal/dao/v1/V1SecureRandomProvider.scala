package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import java.security.SecureRandom

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

trait V1SecureRandomProvider {
  def create: SecureRandom
}

object V1SecureRandomProvider {

  class Default(dynamoDBClientConfig: DynamoDBClientConfig) extends V1SecureRandomProvider {
    override def create: SecureRandom = new SecureRandom()
  }

}
