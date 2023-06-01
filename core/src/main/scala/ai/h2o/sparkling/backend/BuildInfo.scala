package ai.h2o.sparkling.backend

import ai.h2o.sparkling.backend.exceptions.WrongSparkVersion

/**
  * Store information about H2O & Sparkling Water versions so they are available at run-time
  */
object BuildInfo {

  /**
    * Returns Major Spark version for which is this version of Sparkling Water designated.
    *
    * For example, for 1.6.1 returns 1.6
    */
  def buildSparkMajorVersion: String = {
    val VERSION_FILE: String = "/spark.version"
    val stream = getClass.getResourceAsStream(VERSION_FILE)

    stream match {
      case null => throw new WrongSparkVersion(s"Unknown spark version: $VERSION_FILE missing")
      case s =>
        try {
          val version = scala.io.Source.fromInputStream(s).mkString

          if (version.count('.' ==) <= 1) {
            // e.g., 1.6 or "new"
            version
          } else {
            // 1.4
            version.substring(0, version.lastIndexOf('.'))
          }
        } catch {
          case x: Exception =>
            throw new WrongSparkVersion(s"Failed to read spark version from  $VERSION_FILE: ${x.getMessage}")
        }
    }

  }

  /**
    * Returns H2O version used by this Sparkling Water
    */
  def H2OVersion: String = {
    val VERSION_FILE: String = "/h2o.version"
    val stream = getClass.getResourceAsStream(VERSION_FILE)

    stream match {
      case null => throw new RuntimeException(s"Unknown H2O version: $VERSION_FILE missing")
      case s =>
        try {
          scala.io.Source.fromInputStream(s).mkString
        } catch {
          case x: Exception =>
            throw new WrongSparkVersion(s"Failed to read H2O version from  $VERSION_FILE: ${x.getMessage}")
        }
    }
  }

  /**
    * Returns Sparkling Water version
    */
  def SWVersion: String = {
    val VERSION_FILE: String = "/sw.version"
    val stream = getClass.getResourceAsStream(VERSION_FILE)

    stream match {
      case null => throw new RuntimeException(s"Unknown Sparkling Water version: $VERSION_FILE missing")
      case s =>
        try {
          scala.io.Source.fromInputStream(s).mkString
        } catch {
          case x: Exception =>
            throw new WrongSparkVersion(s"Failed to read Sparkling Water version from  $VERSION_FILE: ${x.getMessage}")
        }
    }
  }
}
