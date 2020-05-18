/*
 * Copyright 2017 Dennis Vriend
 * Copyright 2019 Junichi Kato
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

import com.typesafe.config.Config

import scala.language.implicitConversions
import scala.util.Try

object ConfigOps {

  implicit class ConfigOperations(val config: Config) extends AnyVal {

    def exists(key: String): Boolean = {
      Try(config.getAnyRef(key)).map(_ => true).getOrElse(false)
    }

  }

}
