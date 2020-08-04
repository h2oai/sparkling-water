package ai.h2o.sparkling.api.generation

import java.io.{File, PrintWriter}
import java.lang.reflect.{Method, Modifier}
import ai.h2o.sparkling.utils.ScalaUtils.withResource

object ConfigurationRunner {

  val templatesByLanguage = Map("py" -> python.ConfigurationTemplate, "R" -> r.ConfigurationTemplate)
  def main(args: Array[String]): Unit = {
    val destinationDir = args(0)
    val language = args(1)
    val template = templatesByLanguage(language)
    val classes = Array(
      "ai.h2o.sparkling.backend.SharedBackendConf",
      "ai.h2o.sparkling.backend.external.ExternalBackendConf",
      "ai.h2o.sparkling.backend.internal.InternalBackendConf")
    classes.foreach { clazz =>
      writeResultToFile(
        template(getters(clazz), setters(clazz), Class.forName(clazz)),
        clazz.split("\\.").last,
        destinationDir,
        language)
    }
  }

  private val specialSetters = Array("useAutoClusterStart", "useManualClusterStart")

  private def getBaseMethods(className: String): Array[Method] = {
    Class
      .forName(className)
      .getDeclaredMethods()
      .filter(m => Modifier.isPublic(m.getModifiers))
      .filter(m => !Modifier.isStatic(m.getModifiers))
  }

  private def getters(className: String): Array[Method] = {
    getBaseMethods(className).filter(m => !m.getName.startsWith("set") && !specialSetters.contains(m.getName))
  }

  private def setters(className: String): Array[Method] = {
    getBaseMethods(className)
      .filter(m => m.getName.startsWith("set") || specialSetters.contains(m.getName))
      .foldLeft(Array[Method]()) {
        case (acc, method) =>
          if (!acc.exists(_.getName == method.getName)) {
            acc ++ Array(method)
          } else {
            acc
          }
      }
  }

  private def writeResultToFile(content: String, fileName: String, destinationDir: String, language: String) = {
    val destinationDirFile = new File(destinationDir)
    destinationDirFile.mkdirs()
    val destinationFile = new File(destinationDirFile, s"$fileName.$language")
    withResource(new PrintWriter(destinationFile)) { outputStream =>
      outputStream.print(content)
    }
  }
}
