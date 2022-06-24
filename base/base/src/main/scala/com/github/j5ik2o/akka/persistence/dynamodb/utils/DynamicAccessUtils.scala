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

object DynamicAccessUtils {

  def createInstanceFor_CTX_Throw[A: ClassTag, B <: PluginContext](
      className: String,
      pluginContext: B,
      pluginContextClass: Class[_] = classOf[PluginContext]
  ): A = {
    createInstanceFor_CTX[A, B](className, pluginContextClass, pluginContext) match {
      case Success(value) => value
      case Failure(ex) =>
        val className = implicitly[ClassTag[A]].runtimeClass.getName
        throw new PluginException(s"Failed to initialize $className", Some(ex))
    }
  }

  def createInstanceFor_CTX[A: ClassTag, B <: PluginContext](
      className: String,
      pluginContextClass: Class[_],
      pluginContext: B
  ): Try[A] = {
    createInstanceFor_None[A](className, pluginContext.dynamicAccess)
      .recoverWith { case _ =>
        createInstanceFor_DA[A](className, pluginContext.dynamicAccess)
          .recoverWith { case _ =>
            createInstanceFor_PC[A, PluginConfig](
              className,
              pluginContext.dynamicAccess,
              pluginContext.pluginConfig,
              classOf[PluginConfig]
            )
              .recoverWith { case _ =>
                createInstanceFor_DA_PC[A](className, pluginContext.dynamicAccess, pluginContext.pluginConfig)
                  .recoverWith { case _ =>
                    pluginContext.dynamicAccess
                      .createInstanceFor[A](
                        className,
                        Seq(
                          pluginContextClass -> pluginContext
                        )
                      )
                  }
              }
          }
      }
  }

  def createInstanceFor_DA_PC_Throw[A: ClassTag](
      className: String,
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): A = {
    createInstanceFor_DA_PC[A](className, dynamicAccess, pluginConfig) match {
      case Success(value) => value
      case Failure(ex) =>
        val className = implicitly[ClassTag[A]].runtimeClass.getName
        throw new PluginException(s"Failed to initialize $className", Some(ex))
    }
  }

  def createInstanceFor_DA_PC[A: ClassTag](
      className: String,
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): Try[A] = {
    dynamicAccess
      .createInstanceFor[A](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      )
  }

  def createInstanceFor_PC_Throw[A: ClassTag, B <: PluginConfig](
      className: String,
      dynamicAccess: DynamicAccess,
      pluginConfig: B,
      pluginConfigClass: Class[_]
  ): A = {
    createInstanceFor_PC[A, B](className, dynamicAccess, pluginConfig, pluginConfigClass) match {
      case Success(value) => value
      case Failure(ex) =>
        val className = implicitly[ClassTag[A]].runtimeClass.getName
        throw new PluginException(s"Failed to initialize $className", Some(ex))
    }
  }

  def createInstanceFor_PC[A: ClassTag, B <: PluginConfig](
      className: String,
      dynamicAccess: DynamicAccess,
      pluginConfig: B,
      pluginConfigClass: Class[_]
  ): Try[A] = {
    dynamicAccess
      .createInstanceFor[A](
        className,
        Seq(
          pluginConfigClass -> pluginConfig
        )
      )
  }

  def createInstanceFor_DA_Throw[A: ClassTag](
      className: String,
      dynamicAccess: DynamicAccess
  ): A = {
    createInstanceFor_DA[A](className, dynamicAccess) match {
      case Success(value) => value
      case Failure(ex) =>
        val className = implicitly[ClassTag[A]].runtimeClass.getName
        throw new PluginException(s"Failed to initialize $className", Some(ex))
    }
  }

  def createInstanceFor_DA[A: ClassTag](
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

  def createInstanceFor_None_Throw[A: ClassTag](
      className: String,
      dynamicAccess: DynamicAccess
  ): A = {
    createInstanceFor_None[A](className, dynamicAccess) match {
      case Success(value) => value
      case Failure(ex) =>
        val className = implicitly[ClassTag[A]].runtimeClass.getName
        throw new PluginException(s"Failed to initialize $className", Some(ex))
    }
  }

  def createInstanceFor_None[A: ClassTag](
      className: String,
      dynamicAccess: DynamicAccess
  ): Try[A] = {
    dynamicAccess
      .createInstanceFor[A](
        className,
        Seq.empty
      )
  }

}
