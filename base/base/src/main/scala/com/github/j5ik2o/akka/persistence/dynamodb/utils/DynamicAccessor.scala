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
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException

import scala.collection.immutable.Seq
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

abstract class DynamicAccessor[A: ClassTag, B <: PluginContext](val pluginContext: B) {

  def createThrow(className: String): A = {
    create(className) match {
      case Success(value) => value
      case Failure(ex) =>
        val className = implicitly[ClassTag[A]].runtimeClass.getName
        throw new PluginException(s"Failed to initialize $className", Some(ex))
    }
  }

  def create(className: String): Try[A]

  protected def createInstanceFor_None(
      className: String,
      dynamicAccess: DynamicAccess
  ): Try[A] = {
    dynamicAccess
      .createInstanceFor[A](
        className,
        Seq.empty
      )
  }

  protected def createInstanceFor_DA(
      className: String,
      dynamicAccess: DynamicAccess
  ): Try[A] = {
    dynamicAccess
      .createInstanceFor[A](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess
        )
      )
  }

  protected def createInstanceFor_PC[B <: PluginConfig](
      className: String,
      dynamicAccess: DynamicAccess,
      pluginConfig: B,
      pluginConfigClasses: Seq[Class[_]]
  ): Try[A] = {
    def loop(acc: Option[Try[A]], pluginConfigClasses: Seq[Class[_]]): Try[A] = {
      pluginConfigClasses.toList match {
        case Nil => acc.get
        case head :: tail =>
          dynamicAccess
            .createInstanceFor[A](
              className,
              Seq(
                head -> pluginConfig
              )
            ) match {
            case Success(value) => loop(Some(Success(value)), Nil)
            case Failure(ex)    => loop(Some(Failure(ex)), tail)
          }
      }
    }
    loop(None, pluginConfigClasses)
  }

  protected def createInstanceFor_DA_PC[B <: PluginConfig](
      className: String,
      dynamicAccess: DynamicAccess,
      pluginConfig: B,
      pluginConfigClasses: Seq[Class[_]]
  ): Try[A] = {
    def loop(acc: Option[Try[A]], pluginConfigClasses: Seq[Class[_]]): Try[A] = {
      pluginConfigClasses.toList match {
        case Nil => acc.get
        case head :: tail =>
          dynamicAccess
            .createInstanceFor[A](
              className,
              Seq(
                classOf[DynamicAccess] -> dynamicAccess,
                head                   -> pluginConfig
              )
            ) match {
            case Success(value) => loop(Some(Success(value)), Nil)
            case Failure(ex)    => loop(Some(Failure(ex)), tail)
          }
      }
    }
    loop(None, pluginConfigClasses)
  }

  protected def createInstanceFor_CTX(
      className: String,
      dynamicAccess: DynamicAccess,
      pluginContext: B,
      pluginConfigClasses: Seq[Class[_]]
  ): Try[A] = {
    def loop(acc: Option[Try[A]], pluginConfigClasses: Seq[Class[_]]): Try[A] = {
      pluginConfigClasses.toList match {
        case Nil => acc.get
        case head :: tail =>
          dynamicAccess
            .createInstanceFor[A](
              className,
              Seq(
                head -> pluginContext
              )
            ) match {
            case Success(value) => loop(Some(Success(value)), Nil)
            case Failure(ex)    => loop(Some(Failure(ex)), tail)
          }
      }
    }
    loop(None, pluginConfigClasses)
  }
}
