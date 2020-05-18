package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.time.{ Duration => JavaDuration }

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2.V2RetryPolicyProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor

import scala.collection.immutable._
import scala.jdk.CollectionConverters._

object V2ClientOverrideConfigurationUtils {

  def setup(dynamicAccess: DynamicAccess, dynamoDBClientConfig: DynamoDBClientConfig): ClientOverrideConfiguration = {
    import dynamoDBClientConfig.v2ClientConfig._
    var clientOverrideConfigurationBuilder = ClientOverrideConfiguration
      .builder()
    headers.map {
      case (k, v) =>
        clientOverrideConfigurationBuilder.putHeader(k, v.asJava)
    }
    retryMode.foreach { v => clientOverrideConfigurationBuilder = clientOverrideConfigurationBuilder.retryPolicy(v) }
    retryPolicyProviderClassName.foreach { v =>
      val rp = dynamicAccess
        .createInstanceFor[V2RetryPolicyProvider](v, Seq(classOf[DynamoDBClientConfig] -> dynamoDBClientConfig)).get
      clientOverrideConfigurationBuilder = clientOverrideConfigurationBuilder.retryPolicy(rp.create)
    }
    executionInterceptorClassNames.foreach { v =>
      val ei =
        dynamicAccess
          .createInstanceFor[ExecutionInterceptor](v, Seq(classOf[DynamoDBClientConfig] -> dynamoDBClientConfig)).get
      clientOverrideConfigurationBuilder = clientOverrideConfigurationBuilder.addExecutionInterceptor(ei)
    }
    // putAdvancedOption
    apiCallTimeout.foreach { v =>
      clientOverrideConfigurationBuilder =
        clientOverrideConfigurationBuilder.apiCallTimeout(JavaDuration.ofMillis(v.toMillis))
    }
    apiCallAttemptTimeout.foreach { v =>
      clientOverrideConfigurationBuilder =
        clientOverrideConfigurationBuilder.apiCallAttemptTimeout(JavaDuration.ofMillis(v.toMillis))
    }
    // defaultProfileFile
    // defaultProfileName
    clientOverrideConfigurationBuilder.build()
  }
}
