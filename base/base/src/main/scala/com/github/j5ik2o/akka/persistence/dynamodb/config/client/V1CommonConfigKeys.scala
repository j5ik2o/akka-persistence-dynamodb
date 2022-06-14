package com.github.j5ik2o.akka.persistence.dynamodb.config.client

object V1CommonConfigKeys {

  val awsCredentialsProviderProviderClassNameKey =
    "aws-credentials-provider-provider-class-name"
  val awsCredentialsProviderClassNameKey = "aws-credentials-provider-class-name"

  val AWSCredentialsProviderProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.AWSCredentialsProviderProvider"

  val AWSCredentialsProviderClassName =
    "com.amazonaws.auth.AWSCredentialsProvider"

  val DefaultAWSCredentialsProviderProviderClassName: String =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.AWSCredentialsProviderProvider$Default"
}

object V1CommonConfigDefaultValues {}
