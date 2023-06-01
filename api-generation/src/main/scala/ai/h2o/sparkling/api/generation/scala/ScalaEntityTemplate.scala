package ai.h2o.sparkling.api.generation.scala

import ai.h2o.sparkling.api.generation.common.EntitySubstitutionContext

trait ScalaEntityTemplate {
  protected def generateEntity(properties: EntitySubstitutionContext, entityType: String)(content: String): String = {
    s"""package ${properties.namespace}
       |
       |${generateImports(properties)}
       |${properties.annotations.map(annotation => s"\n$annotation").mkString("")}
       |$entityType ${properties.entityName}${properties.parameters}${referencesToInheritedClasses(properties)} {
       |
       |$content
       |}
       |""".stripMargin
  }

  private def generateImports(substitutionContext: EntitySubstitutionContext): String = {
    substitutionContext.imports.map(i => s"import $i").mkString("\n")
  }

  private def referencesToInheritedClasses(substitutionContext: EntitySubstitutionContext): String = {
    if (substitutionContext.inheritedEntities.isEmpty) {
      ""
    } else {
      val head = substitutionContext.inheritedEntities.head
      val tail = substitutionContext.inheritedEntities.tail
      val headResult = s"\n  extends $head"
      val result = Seq(headResult) ++ tail.map(entity => s"\n  with $entity")
      result.mkString("")
    }
  }

  protected def stringify(value: Any): String = value match {
    case f: java.lang.Float => s"${f.toString.toLowerCase}f"
    case d: java.lang.Double => d.toString.toLowerCase
    case l: java.lang.Long => s"${l}L"
    case a: Array[_] => s"Array(${a.map(stringify).mkString(", ")})"
    case s: String => s""""$s""""
    case v if v == null => null
    case v => v.toString
  }
}
