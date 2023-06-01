package ai.h2o.sparkling.api.generation.scala

import ai.h2o.sparkling.api.generation.common._

object AlgorithmTemplate
  extends ((AlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext]) => String)
  with ScalaEntityTemplate {

  def apply(
      algorithmSubstitutionContext: AlgorithmSubstitutionContext,
      parameterSubstitutionContext: Seq[ParameterSubstitutionContext]): String = {
    val entityName = algorithmSubstitutionContext.entityName
    val h2oSchemaClassName = algorithmSubstitutionContext.h2oSchemaClass.getSimpleName
    val parents = Seq(s"${algorithmSubstitutionContext.algorithmType}[${h2oSchemaClassName}]", s"${entityName}Params") ++
      algorithmSubstitutionContext.extraInheritedEntities

    val imports = Seq(
      s"ai.h2o.sparkling.ml.params.${entityName}Params",
      "ai.h2o.sparkling.ml.utils.H2OParamsReadable",
      algorithmSubstitutionContext.h2oSchemaClass.getCanonicalName,
      "org.apache.spark.ml.util.Identifiable",
      s"ai.h2o.sparkling.ml.models.${entityName}MOJOModel",
      "org.apache.spark.sql.Dataset")

    val parameters = "(override val uid: String)"

    val entitySubstitutionContext =
      EntitySubstitutionContext(algorithmSubstitutionContext.namespace, entityName, parents, imports, parameters)

    val algorithmClass = generateEntity(entitySubstitutionContext, "class") {
      s"""  def this() = this(Identifiable.randomUID(classOf[$entityName].getSimpleName))
         |
         |  override def fit(dataset: Dataset[_]): ${entityName}MOJOModel = {
         |    super.fit(dataset).asInstanceOf[${entityName}MOJOModel]
         |  }""".stripMargin
    }

    val algorithmObject = s"object $entityName extends H2OParamsReadable[$entityName]"

    s"""$algorithmClass
       |$algorithmObject
     """.stripMargin
  }
}
