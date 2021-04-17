package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.typesafe.config.Config

import scala.util.Try

object ConfigOps {

  implicit class ConfigOperations(private val config: Config) extends AnyVal {

    def exists(key: String): Boolean = {
      Try(config.getAnyRef(key)).map(_ => true).getOrElse(false)
    }

  }

}
