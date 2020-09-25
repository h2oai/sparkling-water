package ai.h2o.sparkling.api.generation.common

import hex.schemas.Word2VecV3.Word2VecParametersV3
import hex.word2vec.Word2VecModel.Word2VecParameters

trait Word2VecConfiguration extends AlgorithmConfigurations {

  val explicitDefaultValues = Map[String, Any]("model_id" -> null)
  def word2VecParametersSubstitutionContext: ParameterSubstitutionContext = {
    ParameterSubstitutionContext(
      namespace = "ai.h2o.sparkling.ml.params",
      "H2OWord2VecParams",
      classOf[Word2VecParametersV3],
      classOf[Word2VecParameters],
      IgnoredParameters.all("H2OWord2Vec"),
      explicitFields = Seq.empty,
      deprecatedFields = Seq.empty,
      explicitDefaultValues = explicitDefaultValues,
      typeExceptions = Map.empty,
      defaultValueSource = DefaultValueSource.Field,
      defaultValuesOfCommonParameters = defaultValuesOfCommonParameters,
      generateParamTag = true)
  }
}
