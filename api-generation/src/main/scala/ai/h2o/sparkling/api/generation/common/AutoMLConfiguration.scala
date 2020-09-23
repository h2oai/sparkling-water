package ai.h2o.sparkling.api.generation.common

import java.util

import ai.h2o.automl.AutoMLBuildSpec.{AutoMLBuildControl, AutoMLBuildModels, AutoMLInput, AutoMLStoppingCriteria}
import ai.h2o.sparkling.ml.utils.H2OAutoMLSortMetric
import water.automl.api.schemas3.AutoMLBuildSpecV99.{AutoMLBuildControlV99, AutoMLBuildModelsV99, AutoMLInputV99, AutoMLStoppingCriteriaV99}

trait AutoMLConfiguration extends AlgorithmConfigurations {
  def autoMLParameterConfiguration: Seq[ParameterSubstitutionContext] = {
    import DefaultValueSource._

    val autoMLParameters = Seq[(String, Class[_], Class[_], DefaultValueSource)](
      ("H2OAutoMLBuildControlParams", classOf[AutoMLBuildControlV99], classOf[AutoMLBuildControl], Field),
      ("H2OAutoMLInputParams", classOf[AutoMLInputV99], classOf[AutoMLInput], Field),
      ("H2OAutoMLStoppingCriteriaParams", classOf[AutoMLStoppingCriteriaV99], classOf[AutoMLStoppingCriteria], Getter),
      ("H2OAutoMLBuildModelsParams", classOf[AutoMLBuildModelsV99], classOf[AutoMLBuildModels], Field))

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], source) <- autoMLParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        AutoMLIgnoredParameters.all,
        explicitFields = if (entityName == "H2OAutoMLInputParams") Seq(ignoredCols) else Seq.empty,
        deprecatedFields = Seq.empty,
        explicitDefaultValues =
          Map("include_algos" -> ai.h2o.automl.Algo.values().map(_.name()), "response_column" -> "label"),
        defaultValueFieldPrefix = "",
        typeExceptions = Map("sort_metric" -> classOf[H2OAutoMLSortMetric]),
        defaultValueSource = source,
        defaultValuesOfCommonParameters = defaultValuesOfCommonParameters ++
          Map("monotoneConstraints" -> new util.HashMap[String, Double](), "ignoredCols" -> ignoredCols.defaultValue),
        generateParamTag = false)
  }

  def autoMLAlgorithmContext: AlgorithmSubstitutionContext = {
    AlgorithmSubstitutionContext(
      namespace = "ai.h2o.sparkling.ml.algos",
      "H2OAutoML",
      null,
      "H2OSupervisedAlgorithm",
      Seq("H2OAutoMLExtras"),
      false)
  }

  def problemSpecificAutoMLAlgorithmContext: ProblemSpecificAlgorithmSubstitutionContext = {
    ProblemSpecificAlgorithmSubstitutionContext(null, "H2OAutoML", null, "ai.h2o.sparkling.ml.algos", Seq.empty)
  }
}
