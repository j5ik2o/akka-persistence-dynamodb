package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.typesafe.config.{ Config, ConfigRenderOptions }

object ConfigRenderUtils {

  def renderConfigToString(config: Config): String = {
    config.root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(false))
  }

}
