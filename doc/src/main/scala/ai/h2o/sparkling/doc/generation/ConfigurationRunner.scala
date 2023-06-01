package ai.h2o.sparkling.doc.generation

import java.io.{File, PrintWriter}

import ai.h2o.sparkling.utils.ScalaUtils.withResource

object ConfigurationRunner {
  private def writeResultToFile(content: String, fileName: String, destinationDir: String) = {
    val destinationDirFile = new File(destinationDir)
    destinationDirFile.mkdirs()
    val destinationFile = new File(destinationDirFile, s"$fileName.rst")
    withResource(new PrintWriter(destinationFile)) { outputStream =>
      outputStream.print(content)
    }
  }

  def main(args: Array[String]): Unit = {
    val destinationDir = args(0)
    writeResultToFile(ConfigurationsTemplate(), s"configuration_properties", destinationDir)
  }
}
