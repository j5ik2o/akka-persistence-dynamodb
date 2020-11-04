package com.github.j5ik2o.akka.persistence.dynamodb.utils

object ClassCheckUtils extends LoggingSupport {

  def requireClass[A](clazz: Class[A], className: String): String = {
    requireClass(clazz, Some(className)).get
  }

  def requireClass[A](clazz: Class[A], className: Option[String]): Option[String] = {
    try {
      className.foreach { s =>
        require(
          clazz.isAssignableFrom(Class.forName(s)),
          s"`$s` different from the expected the class(${clazz.getName}) was specified."
        )
      }
      className
    } catch {
      case ex: ClassNotFoundException =>
        logger.error(s"The class file of ${clazz.getName} is not found", ex)
        throw ex
    }
  }
}
