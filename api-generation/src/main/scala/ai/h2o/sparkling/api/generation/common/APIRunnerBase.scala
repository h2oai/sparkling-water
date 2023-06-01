package ai.h2o.sparkling.api.generation.common

import java.io.{File, PrintWriter}

import ai.h2o.sparkling.utils.ScalaUtils.withResource

trait APIRunnerBase {
  def writeResultToFile(
      content: String,
      substitutionContext: SubstitutionContextBase,
      languageExtension: String,
      destinationDir: String) = {
    val fileName = substitutionContext.entityName
    val namespacePath = substitutionContext.namespace.replace('.', '/')
    val destinationDirWithNamespace = new File(destinationDir, namespacePath)
    destinationDirWithNamespace.mkdirs()
    val destinationFile = new File(destinationDirWithNamespace, s"$fileName.$languageExtension")
    withResource(new PrintWriter(destinationFile)) { outputStream =>
      outputStream.print(content)
    }
  }
}
