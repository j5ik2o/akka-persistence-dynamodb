package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.time.{ Duration => JavaDuration }

import com.github.j5ik2o.akka.persistence.dynamodb.config.DynamoDBClientConfig
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration

object V2ClientOverrideConfigurationUtils {

  def setup(clientConfig: DynamoDBClientConfig): ClientOverrideConfiguration = {
    var clientOverrideConfigurationBuilder = ClientOverrideConfiguration
      .builder()
    // putHeader
    clientConfig.v2ClientConfig.retryMode.foreach { v =>
      clientOverrideConfigurationBuilder = clientOverrideConfigurationBuilder.retryPolicy(v)
    }
    // executionInterceptors
    // putAdvancedOption
    // apiCallTimeout
    clientConfig.v2ClientConfig.apiCallTimeout.foreach { v =>
      clientOverrideConfigurationBuilder =
        clientOverrideConfigurationBuilder.apiCallTimeout(JavaDuration.ofMillis(v.toMillis))
    }
    // apiCallAttemptTimeout
    clientConfig.v2ClientConfig.apiCallAttemptTimeout.foreach { v =>
      clientOverrideConfigurationBuilder =
        clientOverrideConfigurationBuilder.apiCallAttemptTimeout(JavaDuration.ofMillis(v.toMillis))
    }
    // defaultProfileFile
    // defaultProfileName
    clientOverrideConfigurationBuilder.build()
  }
}
