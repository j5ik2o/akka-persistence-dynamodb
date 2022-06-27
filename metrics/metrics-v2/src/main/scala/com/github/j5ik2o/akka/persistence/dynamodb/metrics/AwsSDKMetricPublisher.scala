/*
 * Copyright 2022 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.core.metrics.CoreMetric
import software.amazon.awssdk.metrics.{ MetricCollection, MetricPublisher }

import scala.annotation.unused
import scala.jdk.StreamConverters._

trait Histogram {
  def record(value: Long): Unit
}

trait Counter {
  def increment(): Unit
  def increment(value: Long): Unit
}

trait AwsSDKMetricPublisherMetrics {
  def putItemApiCallDurationHistogram: Histogram
  def putItemCredentialsFetchDurationHistogram: Histogram
  def putItemMarshallingDurationHistogram: Histogram
  def putItemRetryCounter: Counter
  def putItemErrorCounter: Counter

  def updateItemApiCallDurationHistogram: Histogram
  def updateItemCredentialsFetchDurationHistogram: Histogram
  def updateItemMarshallingDurationHistogram: Histogram
  def updateItemRetryCounter: Counter
  def updateItemErrorCounter: Counter

  def batchWriteItemApiCallDurationHistogram: Histogram
  def batchWriteItemCredentialsFetchDurationHistogram: Histogram
  def batchWriteItemMarshallingDurationHistogram: Histogram
  def batchWriteItemRetryCounter: Counter
  def batchWriteItemErrorCounter: Counter

  def queryApiCallDurationHistogram: Histogram
  def queryCredentialsFetchDurationHistogram: Histogram
  def queryMarshallingDurationHistogram: Histogram
  def queryRetryCounter: Counter
  def queryErrorCounter: Counter
}

final class AwsSDKMetricPublisher(
    pluginConfig: PluginConfig,
    awsSDKMetricPublisherMetrics: AwsSDKMetricPublisherMetrics
) extends MetricPublisher {
  import awsSDKMetricPublisherMetrics._

  override def publish(metricCollection: MetricCollection): Unit = {
    val metricsMap = metricCollection.stream().toScala(Vector).map { mr => (mr.metric().name(), mr) }.toMap
    metricsMap(CoreMetric.OPERATION_NAME.name()).value() match {
      case "PutItem" =>
        if (metricsMap(CoreMetric.API_CALL_SUCCESSFUL.name()).value().asInstanceOf[java.lang.Boolean]) {
          val apiCallDuration = metricsMap(CoreMetric.API_CALL_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val credentialsFetchDuration =
            metricsMap(CoreMetric.CREDENTIALS_FETCH_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val marshallingDuration =
            metricsMap(CoreMetric.MARSHALLING_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val retryCount = metricsMap(CoreMetric.RETRY_COUNT.name()).value().asInstanceOf[java.lang.Integer]

          putItemApiCallDurationHistogram.record(apiCallDuration.toMillis)
          putItemCredentialsFetchDurationHistogram.record(credentialsFetchDuration.toMillis)
          putItemMarshallingDurationHistogram.record(marshallingDuration.toMillis)
          putItemRetryCounter.increment(retryCount.toLong)
        } else {
          putItemErrorCounter.increment()
        }
      case "UpdateItem" =>
        if (metricsMap(CoreMetric.API_CALL_SUCCESSFUL.name()).value().asInstanceOf[java.lang.Boolean]) {
          val apiCallDuration = metricsMap(CoreMetric.API_CALL_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val credentialsFetchDuration =
            metricsMap(CoreMetric.CREDENTIALS_FETCH_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val marshallingDuration =
            metricsMap(CoreMetric.MARSHALLING_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val retryCount = metricsMap(CoreMetric.RETRY_COUNT.name()).value().asInstanceOf[java.lang.Integer]

          updateItemApiCallDurationHistogram.record(apiCallDuration.toMillis)
          updateItemCredentialsFetchDurationHistogram.record(credentialsFetchDuration.toMillis)
          updateItemMarshallingDurationHistogram.record(marshallingDuration.toMillis)
          updateItemRetryCounter.increment(retryCount.toLong)
        } else {
          batchWriteItemErrorCounter.increment()
        }
      case "BatchWriteItem" =>
        if (metricsMap(CoreMetric.API_CALL_SUCCESSFUL.name()).value().asInstanceOf[java.lang.Boolean]) {
          val apiCallDuration = metricsMap(CoreMetric.API_CALL_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val credentialsFetchDuration =
            metricsMap(CoreMetric.CREDENTIALS_FETCH_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val marshallingDuration =
            metricsMap(CoreMetric.MARSHALLING_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val retryCount = metricsMap(CoreMetric.RETRY_COUNT.name()).value().asInstanceOf[java.lang.Integer]

          batchWriteItemApiCallDurationHistogram.record(apiCallDuration.toMillis)
          batchWriteItemCredentialsFetchDurationHistogram.record(credentialsFetchDuration.toMillis)
          batchWriteItemMarshallingDurationHistogram.record(marshallingDuration.toMillis)
          batchWriteItemRetryCounter.increment(retryCount.toLong)
        } else {
          batchWriteItemErrorCounter.increment()
        }
      case "Query" =>
        if (metricsMap(CoreMetric.API_CALL_SUCCESSFUL.name()).value().asInstanceOf[java.lang.Boolean]) {
          val apiCallDuration = metricsMap(CoreMetric.API_CALL_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val credentialsFetchDuration =
            metricsMap(CoreMetric.CREDENTIALS_FETCH_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val marshallingDuration =
            metricsMap(CoreMetric.MARSHALLING_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val retryCount = metricsMap(CoreMetric.RETRY_COUNT.name()).value().asInstanceOf[java.lang.Integer]

          queryApiCallDurationHistogram.record(apiCallDuration.toMillis)
          queryCredentialsFetchDurationHistogram.record(credentialsFetchDuration.toMillis)
          queryMarshallingDurationHistogram.record(marshallingDuration.toMillis)
          queryRetryCounter.increment(retryCount.toLong)
        } else {
          queryErrorCounter.increment()
        }
      case _ =>
    }
  }

  override def close(): Unit = {}
}
