package com.github.j5ik2o.akka.persistence.dynamodb.utils

object ClassCheckUtils extends LoggingSupport {

  def requireClassByName(expectedClassName: String, className: String): String = {
    requireClassByName(expectedClassName, Some(className)).get
  }

  def requireClass[A](expectedClass: Class[A], className: String): String = {
    requireClass[A](expectedClass, Some(className)).get
  }

  def requireClassByName(expectedClassName: String, classNameOpt: Option[String]): Option[String] = {
    requireClass(Class.forName(expectedClassName), classNameOpt)
  }

  def requireClass[A](expectedClass: Class[A], classNameOpt: Option[String]): Option[String] = {
    try {
      classNameOpt.foreach { s =>
        require(
          expectedClass.isAssignableFrom(Class.forName(s)),
          s"`$s` different from the expected the class(${expectedClass.getName}) was specified."
        )
      }
      classNameOpt
    } catch {
      case ex: ClassNotFoundException =>
        logger.error(s"The class file of ${expectedClass.getName} is not found", ex)
        throw ex
    }
  }
}
