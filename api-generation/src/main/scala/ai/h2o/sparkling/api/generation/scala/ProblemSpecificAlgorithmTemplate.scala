package ai.h2o.sparkling.api.generation.scala

import ai.h2o.sparkling.api.generation.common._
import ai.h2o.sparkling.api.generation.python.AlgorithmTemplate.resolveParameters

object ProblemSpecificAlgorithmTemplate
  extends ((String, ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext]) => String)
  with ScalaEntityTemplate {

  def apply(
      problemType: String,
      algorithmSubstitutionContext: ProblemSpecificAlgorithmSubstitutionContext,
      parameterSubstitutionContexts: Seq[ParameterSubstitutionContext]): String = {
    val parameterNames = parameterSubstitutionContexts.flatMap(resolveParameters).map(_.h2oName)
    val parentEntityName = algorithmSubstitutionContext.parentEntityName
    val parentNamespace = algorithmSubstitutionContext.parentNamespace
    val entityName = algorithmSubstitutionContext.entityName
    val namespace = algorithmSubstitutionContext.namespace
    val problemTypeClass = "H2O" + entityName.substring(parentEntityName.length)

    val checks = algorithmSubstitutionContext.parametersToCheck.map { parameter =>
      s"${parameter.capitalize}For${problemType.capitalize}Check"
    }
    val parents = Seq(parentEntityName, problemTypeClass) ++ checks

    val imports = Seq(
      s"${parentNamespace}.${parentEntityName}",
      "ai.h2o.sparkling.ml.utils.H2OParamsReadable",
      "org.apache.spark.ml.util.Identifiable")

    val classParameters = "(override val uid: String)"

    val entitySubstitutionContext =
      EntitySubstitutionContext(namespace, entityName, parents, imports, classParameters)

    val algorithmClass = generateEntity(entitySubstitutionContext, "class") {
      s"  def this() = this(Identifiable.randomUID(classOf[$entityName].getSimpleName))"
    }

    val algorithmObject = s"object $entityName extends H2OParamsReadable[$entityName]"

    s"""$algorithmClass
       |$algorithmObject
     """.stripMargin
  }
}
