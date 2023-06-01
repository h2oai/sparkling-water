package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common._

object ProblemSpecificAlgorithmTemplate
  extends ((String, ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext]) => String)
  with AlgorithmTemplateBase
  with ParameterResolver {

  def apply(
      problemType: String,
      algorithmSubstitutionContext: ProblemSpecificAlgorithmSubstitutionContext,
      parameterSubstitutionContexts: Seq[ParameterSubstitutionContext]): String = {
    val parameters = parameterSubstitutionContexts.flatMap(resolveParameters)
    val entityName = algorithmSubstitutionContext.entityName
    val namespace = algorithmSubstitutionContext.namespace
    val parentEntityName = algorithmSubstitutionContext.parentEntityName
    val parentNamespace = algorithmSubstitutionContext.parentNamespace
    val parents = Seq(parentEntityName)

    val imports = Seq(
      "warnings.warn",
      "pyspark.keyword_only",
      "ai.h2o.sparkling.Initializer.Initializer",
      "ai.h2o.sparkling.ml.Utils.Utils",
      s"$parentNamespace.$parentEntityName.$parentEntityName")

    val entitySubstitutionContext = EntitySubstitutionContext(namespace, entityName, parents, imports)

    generateAlgorithmClass(
      entityName,
      parentEntityName,
      namespace,
      parameters,
      entitySubstitutionContext,
      parameterSubstitutionContexts)
  }
}
