/*
 * Copyright 2021 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.typesafe.config.{ Config, ConfigException }

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

object ConfigOps {

  implicit class ConfigOperations(private val config: Config) extends AnyVal {

    def exists(key: String): Boolean = {
      Try(config.getAnyRef(key)).map(_ => true).getOrElse(false)
    }

    def configAs(key: String, defaultConfig: Config): Config = {
      try {
        config.getConfig(key)
      } catch {
        case _: ConfigException.Missing =>
          defaultConfig
      }
    }

    def value[A](key: String)(implicit classTag: ClassTag[A]): A = {
      classTag.runtimeClass match {
        case clazz if classOf[Duration].isAssignableFrom(clazz) =>
          val result = config.getDuration(key)
          result.toMillis.milliseconds.asInstanceOf[A]
        case clazz if clazz == classOf[Long] =>
          val result = config.getLong(key)
          result.asInstanceOf[A]
        case clazz if clazz == classOf[Int] =>
          val result = config.getInt(key)
          result.asInstanceOf[A]
        case _ =>
          val result = config.getAnyRef(key)
          result.asInstanceOf[A]
      }
    }

    def valueAs[A](key: String, defaultValue: => A): A = {
      try {
        defaultValue match {
          case _: Duration =>
            val result = config.getDuration(key)
            result.toMillis.milliseconds.asInstanceOf[A]
          case _: Long =>
            val result = config.getLong(key)
            result.asInstanceOf[A]
          case _: Int =>
            val result = config.getInt(key)
            result.asInstanceOf[A]
          case _ =>
            val result = config.getAnyRef(key)
            result.asInstanceOf[A]
        }
      } catch {
        case _: ConfigException.Missing =>
          defaultValue
      }
    }

    def valueOptAs[A](key: String): Option[A] = {
      if (config.hasPath(key)) {
        val value = config.getAnyRef(key)
        Some(value.asInstanceOf[A])
      } else {
        None
      }
    }

    def valuesAs[A](key: String, defaultValue: scala.collection.Seq[A]): scala.collection.Seq[A] = {
      try {
        config.getAnyRefList(key).asScala.map(_.asInstanceOf[A]).toVector
      } catch {
        case _: ConfigException.Missing =>
          defaultValue
      }
    }

    def mapAs[A](
        key: String,
        defaultValue: scala.collection.Map[String, scala.collection.Seq[A]]
    ): scala.collection.Map[String, scala.collection.Seq[A]] = {
      try {
        val relativeConfig = config.getConfig(key)
        relativeConfig
          .root().entrySet().asScala.map { entry =>
            val key = entry.getKey
            key -> relativeConfig.getAnyRefList(key).asScala.map(_.asInstanceOf[A]).toSeq
          }.toMap
      } catch {
        case _: ConfigException.Missing =>
          defaultValue
      }
    }

  }

}
