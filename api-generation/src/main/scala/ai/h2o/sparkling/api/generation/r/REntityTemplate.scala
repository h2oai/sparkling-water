package ai.h2o.sparkling.api.generation.r

import ai.h2o.sparkling.api.generation.common.EntitySubstitutionContext

trait REntityTemplate {
  protected def generateEntity(properties: EntitySubstitutionContext)(content: String): String = {
    s"""${generateImports(properties)}
       |
       |#' @export ${properties.entityName}
       |${properties.entityName} <- setRefClass("${properties.entityName}", ${referencesToInheritedClasses(properties)}methods = list(
       |$content
       |))
       |""".stripMargin
  }

  protected def generateImports(substitutionContext: EntitySubstitutionContext): String = {
    substitutionContext.imports.map(i => s"""source(file.path("R", "$i.R"))""").mkString("\n")
  }

  private def referencesToInheritedClasses(substitutionContext: EntitySubstitutionContext): String = {
    if (substitutionContext.inheritedEntities.isEmpty) {
      ""
    } else {
      substitutionContext.inheritedEntities.mkString("""contains = ("""", """", """"", """"), """)
    }
  }
}
