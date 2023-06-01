package ai.h2o.sparkling.api.generation.common

import hex.Model.Parameters.FoldAssignmentScheme
import hex.ensemble.StackedEnsembleModel.StackedEnsembleParameters
import hex.schemas.StackedEnsembleModelV99.StackedEnsembleModelOutputV99
import hex.schemas.StackedEnsembleV99.StackedEnsembleParametersV99

class StackedEnsembleConfiguration extends SingleAlgorithmConfiguration {

  override def parametersConfiguration: Seq[ParameterSubstitutionContext] = {

    val stackedEnsembleParameters = Seq[(String, Class[_], Class[_])](
      ("H2OStackedEnsembleParams", classOf[StackedEnsembleParametersV99], classOf[StackedEnsembleParameters]))

    val blendingFrame = ExplicitField("blending_frame", "HasBlendingDataFrame", null, Some("blendingDataFrame"), None)
    val baseAlgorithms = ExplicitField("base_algorithms", "HasBaseAlgorithms", null, Some("baseAlgorithms"), None)

    val explicitDefaultValues =
      Map[String, Any]("model_id" -> null, "metalearner_fold_assignment" -> FoldAssignmentScheme.AUTO)

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_]) <- stackedEnsembleParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        ignoredParameters = Seq("__meta", "base_models", "training_frame", "validation_frame"),
        explicitFields = Seq(blendingFrame, baseAlgorithms),
        deprecatedFields = Seq.empty,
        explicitDefaultValues,
        typeExceptions = Map.empty,
        defaultValueSource = DefaultValueSource.Field,
        defaultValuesOfCommonParameters = AlgorithmConfigurations.defaultValuesOfAlgorithmCommonParameters,
        generateParamTag = false)
  }

  override def algorithmConfiguration: Seq[AlgorithmSubstitutionContext] = {
    Seq(
      AlgorithmSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.algos",
        "H2OStackedEnsemble",
        null,
        "H2OSupervisedAlgorithm",
        Seq("H2OStackedEnsembleExtras"),
        false))
  }

  override def modelOutputConfiguration: Seq[ModelOutputSubstitutionContext] = {
    val modelOutputs =
      Seq[(String, Class[_])](("H2OStackedEnsembleModelOutputs", classOf[StackedEnsembleModelOutputV99]))

    for ((outputEntityName, h2oParametersClass: Class[_]) <- modelOutputs)
      yield ModelOutputSubstitutionContext(
        "ai.h2o.sparkling.ml.outputs",
        outputEntityName,
        h2oParametersClass,
        Seq.empty)
  }
}
