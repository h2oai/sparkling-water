package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common._

object AlgorithmTemplate
  extends ((AlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext]) => String)
  with AlgorithmTemplateBase
  with ParameterResolver {

  def apply(
      algorithmSubstitutionContext: AlgorithmSubstitutionContext,
      parameterSubstitutionContexts: Seq[ParameterSubstitutionContext]): String = {
    val parameters = parameterSubstitutionContexts.flatMap(resolveParameters)
    val entityName = algorithmSubstitutionContext.entityName
    val namespace = algorithmSubstitutionContext.namespace
    val paramClasses = Seq(s"${entityName}Params")
    val algorithmType = algorithmSubstitutionContext.algorithmType
    val parents = paramClasses ++ Seq(algorithmType) ++ algorithmSubstitutionContext.extraInheritedEntities
    val mojoClassName = s"${entityName}MOJOModel"
    val constructorMethods = algorithmSubstitutionContext.constructorMethods

    val imports = Seq(
      "warnings.warn",
      "pyspark.keyword_only",
      "ai.h2o.sparkling.Initializer.Initializer",
      "ai.h2o.sparkling.ml.Utils.Utils",
      s"$namespace.$algorithmType.$algorithmType") ++
      (if (constructorMethods) Seq(s"ai.h2o.sparkling.ml.models.$mojoClassName.$mojoClassName") else Seq.empty) ++
      paramClasses.map(clazz => s"ai.h2o.sparkling.ml.params.$clazz.$clazz") ++
      algorithmSubstitutionContext.extraInheritedEntities.map(clazz => s"ai.h2o.sparkling.ml.algos.$clazz.$clazz")

    val entitySubstitutionContext = EntitySubstitutionContext(namespace, entityName, parents, imports)

    val additionalMethod = if (constructorMethods) {
      s"""
         |    def _create_model(self, javaModel):
         |        return ${entityName}MOJOModel(javaModel)
         |""".stripMargin
    } else {
      ""
    }

    val clazz =
      generateAlgorithmClass(
        entityName,
        entityName,
        namespace,
        parameters,
        entitySubstitutionContext,
        parameterSubstitutionContexts)

    clazz + additionalMethod
  }
}
