package ai.h2o.sparkling.api.generation.scala

import ai.h2o.sparkling.api.generation.common.{AlgorithmSubstitutionContext, EntitySubstitutionContext}

object MOJOModelFactoryTemplate extends ((Seq[AlgorithmSubstitutionContext]) => String) with ScalaEntityTemplate {

  def apply(mojoSubstitutionContexts: Seq[AlgorithmSubstitutionContext]): String = {

    val entitySubstitutionContext = EntitySubstitutionContext(
      mojoSubstitutionContexts.head.namespace,
      "H2OMOJOModelFactory",
      inheritedEntities = Seq.empty,
      imports = Seq.empty)

    generateEntity(entitySubstitutionContext, "trait") {
      s"""  protected def createSpecificMOJOModel(uid: String, algorithmName: String, category: hex.ModelCategory): H2OMOJOModel = {
         |    (algorithmName, category) match {
         |      case (_, hex.ModelCategory.AutoEncoder) => new H2OAutoEncoderMOJOModel(uid)
         |${generatePatternMatchingCases(mojoSubstitutionContexts)}
         |      case _ => new H2OAlgorithmMOJOModel(uid)
         |    }
         |  }""".stripMargin
    }
  }

  private def generatePatternMatchingCases(mojoSubstitutionContexts: Seq[AlgorithmSubstitutionContext]): String = {
    mojoSubstitutionContexts
      .map { mojoSubstitutionContext =>
        val algorithmName = mojoSubstitutionContext.entityName
          .replace("H2O", "")
          .replace("MOJOModel", "")
          .toLowerCase
        s"""      case ("$algorithmName", _) => new ${mojoSubstitutionContext.entityName}(uid)"""
      }
      .mkString("\n")
  }
}
