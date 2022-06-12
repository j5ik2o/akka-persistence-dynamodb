/*
 * Copyright 2020 Junichi Kato
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

object ClassCheckUtils extends LoggingSupport {

  def requireClassByName(expectedClassName: String, className: String, validation: Boolean): String = {
    requireClassByName(expectedClassName, Some(className), validation).get
    className
  }

  def requireClassByName(
      expectedClassName: String,
      classNameOpt: Option[String],
      validation: Boolean
  ): Option[String] = {
    if (validation)
      requireClass(Class.forName(expectedClassName), classNameOpt, validation)
    classNameOpt
  }

  def requireClass[A](expectedClass: Class[A], className: String): String =
    requireClass[A](expectedClass, className, true)

  def requireClass[A](expectedClass: Class[A], className: String, validation: Boolean): String = {
    requireClass[A](expectedClass, Some(className), validation).get
    className
  }

  def requireClass[A](expectedClass: Class[A], classNameOpt: Option[String]): Option[String] =
    requireClass[A](expectedClass, classNameOpt, true)

  def requireClass[A](expectedClass: Class[A], classNameOpt: Option[String], validation: Boolean): Option[String] = {
    try {
      if (validation) {
        classNameOpt.foreach { s =>
          require(
            expectedClass.isAssignableFrom(Class.forName(s)),
            s"`$s` different from the expected the class(${expectedClass.getName}) was specified."
          )
        }
      }
      classNameOpt
    } catch {
      case ex: ClassNotFoundException =>
        logger.error(s"The class file of ${expectedClass.getName} is not found", ex)
        throw ex
    }
  }
}
