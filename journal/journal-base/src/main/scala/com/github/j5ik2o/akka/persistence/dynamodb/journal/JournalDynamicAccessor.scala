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
package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.journal.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowWriteDriver
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessor

import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

object JournalDynamicAccessor {
  def apply[A: ClassTag](pluginContext: JournalPluginContext): JournalDynamicAccessor[A] =
    new JournalDynamicAccessor[A](pluginContext)
}

final class JournalDynamicAccessor[A: ClassTag](pluginContext: JournalPluginContext)
    extends DynamicAccessor[A, JournalPluginContext](pluginContext) {
  override def create(className: String): Try[A] = {
    createInstanceFor_None(className, pluginContext.dynamicAccess)
      .recoverWith { case _ =>
        createInstanceFor_DA(className, pluginContext.dynamicAccess)
          .recoverWith { case _ =>
            createInstanceFor_PC[JournalPluginConfig](
              className,
              pluginContext.dynamicAccess,
              pluginContext.pluginConfig,
              Vector(classOf[PluginConfig], classOf[JournalPluginConfig])
            ).recoverWith { case _ =>
              createInstanceFor_DA_PC[JournalPluginConfig](
                className,
                pluginContext.dynamicAccess,
                pluginContext.pluginConfig,
                Vector(classOf[PluginConfig], classOf[JournalPluginConfig])
              ).recoverWith { case _ =>
                createInstanceFor_PC_JRD(
                  className,
                  pluginContext.dynamicAccess,
                  pluginContext.journalRowWriteDriver,
                  pluginContext.pluginConfig,
                  Vector(
                    classOf[PluginConfig],
                    classOf[JournalPluginConfig]
                  )
                ).recoverWith { case _ =>
                  createInstanceFor_CTX(
                    className,
                    pluginContext.dynamicAccess,
                    pluginContext,
                    Vector(
                      classOf[PluginContext],
                      classOf[JournalPluginContext]
                    )
                  )
                }
              }
            }
          }
      }
  }

  protected def createInstanceFor_PC_JRD[B <: PluginConfig](
      className: String,
      dynamicAccess: DynamicAccess,
      journalRowDriver: JournalRowWriteDriver,
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
              Vector(
                head                           -> pluginConfig,
                classOf[JournalRowWriteDriver] -> journalRowDriver
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
