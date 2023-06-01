package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common.EntitySubstitutionContext

trait PythonEntityTemplate {
  protected def generateEntity(properties: EntitySubstitutionContext)(content: String): String = {
    s"""${generateImports(properties)}
       |
       |
       |class ${properties.entityName}${referencesToInheritedClasses(properties)}:
       |
       |$content
       |""".stripMargin
  }

  protected def generateImports(substitutionContext: EntitySubstitutionContext): String = {
    substitutionContext.imports
      .map { i =>
        val parts = i.split('.')
        val namespace = parts.take(parts.length - 1).mkString(".")
        val clazz = parts.last
        s"from $namespace import $clazz"
      }
      .mkString("\n")
  }

  private def referencesToInheritedClasses(substitutionContext: EntitySubstitutionContext): String = {
    if (substitutionContext.inheritedEntities.isEmpty) {
      ""
    } else {
      substitutionContext.inheritedEntities.mkString("(", ", ", ")")
    }
  }

  protected def stringify(value: Any): String = value match {
    case a: Array[_] => s"[${a.map(stringify).mkString(", ")}]"
    case b: Boolean => b.toString.capitalize
    case s: String => s""""$s""""
    case v if v == null => "None"
    case v: Enum[_] => s""""$v""""
    case v => v.toString
  }
}
