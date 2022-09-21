/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.h2o.sparkling.api.generation.common

import hex.coxph.CoxPHModel.CoxPHParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.gam.GAMModel.GAMParameters
import hex.glm.GLMModel.GLMParameters
import hex.kmeans.KMeansModel.KMeansParameters
import hex.rulefit.RuleFitModel.RuleFitParameters
import hex.schemas.CoxPHModelV3.CoxPHModelOutputV3
import hex.schemas.CoxPHV3.CoxPHParametersV3
import hex.schemas.DRFModelV3.DRFModelOutputV3
import hex.schemas.DeepLearningModelV3.DeepLearningModelOutputV3
import hex.schemas.DeepLearningV3.{DeepLearningParametersV3 => DLParamsV3}
import hex.schemas.ExtendedIsolationForestModelV3.ExtendedIsolationForestModelOutputV3
import hex.schemas.GAMModelV3.GAMModelOutputV3
import hex.schemas.GBMModelV3.GBMModelOutputV3
import hex.schemas.GLMModelV3.GLMModelOutputV3
import hex.schemas.IsolationForestModelV3.IsolationForestModelOutputV3
import hex.schemas.IsolationForestV3.{IsolationForestParametersV3 => IFParamsV3}
import hex.schemas.ExtendedIsolationForestV3.{ExtendedIsolationForestParametersV3 => ExtIFParamsV3}
import hex.schemas.KMeansModelV3.KMeansModelOutputV3
import hex.schemas.KMeansV3.{KMeansParametersV3 => KMeansParamsV3}
import hex.schemas.RuleFitModelV3.RuleFitModelOutputV3
import hex.schemas.RuleFitV3.RuleFitParametersV3
import hex.schemas.XGBoostModelV3.XGBoostModelOutputV3
import hex.schemas.XGBoostV3.{XGBoostParametersV3 => XGBParamsV3}
import hex.schemas.{DRFV3, GAMV3, GBMV3, GLMV3}
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.isofor.IsolationForestModel.{IsolationForestParameters => IFParameters}
import hex.tree.isoforextended.ExtendedIsolationForestModel.{ExtendedIsolationForestParameters => ExtIFParams}
import hex.tree.xgboost.XGBoostModel.XGBoostParameters

import java.util

object AlgorithmConfigurations {

  val defaultValuesOfCommonParameters = Map(
    "convertUnknownCategoricalLevelsToNa" -> false,
    "convertInvalidNumbersToNa" -> false,
    "validationDataFrame" -> null,
    "splitRatio" -> 1.0,
    "columnsToCategorical" -> Array.empty[String],
    "keepBinaryModels" -> false,
    "dataFrameSerializer" -> "ai.h2o.sparkling.utils.JSONDataFrameSerializer")

  val defaultValuesOfAlgorithmCommonParameters = Map(
    "featuresCols" -> Array.empty[String],
    "predictionCol" -> "prediction",
    "detailedPredictionCol" -> "detailed_prediction",
    "namedMojoOutputColumns" -> true,
    "withContributions" -> false,
    "withLeafNodeAssignments" -> false,
    "withStageResults" -> false) ++ defaultValuesOfCommonParameters

  val ignoredCols = ExplicitField("ignored_columns", "HasIgnoredCols", null, None, Some("HasIgnoredColsOnMOJO"))
}

class AlgorithmConfigurations extends MultipleAlgorithmsConfiguration {

  import AlgorithmConfigurations.ignoredCols

  override def parametersConfiguration: Seq[ParameterSubstitutionContext] = {
    val monotonicity = ExplicitField(
      "monotone_constraints",
      "HasMonotoneConstraints",
      new util.HashMap[String, Double](),
      None,
      Some("HasMonotoneConstraintsOnMOJO"))
    val calibrationDataFrame = ExplicitField("calibration_frame", "HasCalibrationDataFrame", null)
    val plugValues = ExplicitField("plug_values", "HasPlugValues", null)
    val betaConstraints = ExplicitField("beta_constraints", "HasBetaConstraints", null)
    val userPoints = ExplicitField("user_points", "HasUserPoints", null)
    val randomCols = ExplicitField("random_columns", "HasRandomCols", null)
    val gamCols = ExplicitField("gam_columns", "HasGamCols", null, None, Some("HasGamColsOnMOJO"))
    val validationLabelCol = ExplicitField("validation_response_column", "HasValidationLabelCol", "label")
    val interactionPairs = ExplicitField("interaction_pairs", "HasInteractionPairs", null)

    val xgboostFields = Seq(monotonicity, calibrationDataFrame, ignoredCols)
    val glmFields = Seq(randomCols, ignoredCols, plugValues, betaConstraints, interactionPairs)
    val gamFields = Seq(ignoredCols, betaConstraints, gamCols)
    val gbmFields = Seq(monotonicity, calibrationDataFrame, ignoredCols)
    val drfFields = Seq(calibrationDataFrame, ignoredCols)
    val kmeansFields = Seq(userPoints, ignoredCols)
    val coxPHFields = Seq(ignoredCols, interactionPairs)
    val ifFields = Seq(ignoredCols, calibrationDataFrame, validationLabelCol)
    val extIfFields = Seq(ignoredCols)

    val ruleFitFields = Seq(ExplicitField("offset_column", "HasUnsupportedOffsetCol", null), ignoredCols)

    val dlFields = Seq(
      ExplicitField("initial_biases", "HasInitialBiases", null),
      ExplicitField("initial_weights", "HasInitialWeights", null),
      ignoredCols)

    val explicitDefaultValues =
      Map[String, Any]("max_w2" -> 3.402823e38f, "response_column" -> "label", "model_id" -> null, "lambda" -> null)

    val noDeprecation = Seq.empty

    val dlDeprecations = Seq(
      DeprecatedField(
        "variable_importances",
        "HasDeprecatedVariableImportances",
        "variableImportances",
        "3.38",
        Some("calculateFeatureImportances"),
        Some("HasDeprecatedVariableImportancesOnMOJO")),
      DeprecatedField(
        "autoencoder",
        "HasDeprecatedAutoencoder",
        "autoencoder",
        "3.38",
        None,
        Some("HasDeprecatedAutoencoderOnMOJO")))

    val algorithmParameters = Seq[(String, Class[_], Class[_], Seq[ExplicitField], Seq[DeprecatedField])](
      ("H2OXGBoostParams", classOf[XGBParamsV3], classOf[XGBoostParameters], xgboostFields, noDeprecation),
      ("H2OGBMParams", classOf[GBMV3.GBMParametersV3], classOf[GBMParameters], gbmFields, noDeprecation),
      ("H2ODRFParams", classOf[DRFV3.DRFParametersV3], classOf[DRFParameters], drfFields, noDeprecation),
      ("H2OGLMParams", classOf[GLMV3.GLMParametersV3], classOf[GLMParameters], glmFields, noDeprecation),
      ("H2OGAMParams", classOf[GAMV3.GAMParametersV3], classOf[GAMParameters], gamFields, noDeprecation),
      ("H2ODeepLearningParams", classOf[DLParamsV3], classOf[DeepLearningParameters], dlFields, dlDeprecations),
      ("H2ORuleFitParams", classOf[RuleFitParametersV3], classOf[RuleFitParameters], ruleFitFields, noDeprecation),
      ("H2OKMeansParams", classOf[KMeansParamsV3], classOf[KMeansParameters], kmeansFields, noDeprecation),
      ("H2OCoxPHParams", classOf[CoxPHParametersV3], classOf[CoxPHParameters], coxPHFields, noDeprecation),
      ("H2OIsolationForestParams", classOf[IFParamsV3], classOf[IFParameters], ifFields, noDeprecation),
      ("H2OExtendedIsolationForestParams", classOf[ExtIFParamsV3], classOf[ExtIFParams], extIfFields, noDeprecation))

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], explicitFields, deprecatedFields) <- algorithmParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        IgnoredParameters.all(entityName.replace("Params", "")),
        explicitFields,
        deprecatedFields,
        explicitDefaultValues,
        typeExceptions = Map.empty,
        defaultValueSource = DefaultValueSource.Field,
        defaultValuesOfCommonParameters = AlgorithmConfigurations.defaultValuesOfAlgorithmCommonParameters,
        generateParamTag = true)
  }

  override def algorithmConfiguration: Seq[AlgorithmSubstitutionContext] = {

    val withDistribution = "DistributionBasedH2OTrainFramePreparation"
    val withFamily = "FamilyBasedH2OTrainFramePreparation"

    val treeSupervised = "H2OTreeBasedSupervisedAlgorithm"
    val supervised = "H2OSupervisedAlgorithm"
    val cvSupervised = "H2OSupervisedAlgorithmWithFoldColumn"
    val unsupervised = "H2OUnsupervisedAlgorithm"
    val treeUnsupervised = "H2OTreeBasedUnsupervisedAlgorithm"

    val algorithms = Seq[(String, Class[_], String, Seq[String], Option[String])](
      ("H2OXGBoost", classOf[XGBoostParameters], treeSupervised, Seq(withDistribution), None),
      ("H2OGBM", classOf[GBMParameters], treeSupervised, Seq(withDistribution), None),
      ("H2ODRF", classOf[DRFParameters], treeSupervised, Seq(withDistribution), None),
      ("H2OGLM", classOf[GLMParameters], cvSupervised, Seq(withFamily), Some("H2OGLMMetrics")),
      ("H2OGAM", classOf[GAMParameters], cvSupervised, Seq(withFamily), None),
      ("H2ODeepLearning", classOf[DeepLearningParameters], cvSupervised, Seq(withDistribution), None),
      ("H2ORuleFit", classOf[RuleFitParameters], supervised, Seq(withDistribution), None),
      ("H2OKMeans", classOf[KMeansParameters], unsupervised, Seq("H2OKMeansExtras"), Some("H2OClusteringMetrics")),
      ("H2OCoxPH", classOf[CoxPHParameters], supervised, Seq.empty, Some("H2ORegressionCoxPHMetrics")),
      ("H2OIsolationForest", classOf[IFParameters], treeUnsupervised, Seq.empty, Some("H2OAnomalyMetrics")),
      ("H2OExtendedIsolationForest", classOf[ExtIFParams], treeUnsupervised, Seq.empty, Some("H2OAnomalyMetrics")))

    for ((entityName, h2oParametersClass: Class[_], algorithmType, extraParents, metricsClass) <- algorithms)
      yield AlgorithmSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.algos",
        entityName,
        h2oParametersClass,
        algorithmType,
        extraParents,
        specificMetricsClass = metricsClass)
  }

  override def problemSpecificAlgorithmConfiguration: Seq[ProblemSpecificAlgorithmSubstitutionContext] = {

    val algorithms = Seq[(String, Seq[String])](
      ("H2OXGBoost", Seq("distribution")),
      ("H2OGBM", Seq("distribution")),
      ("H2ODRF", Seq("distribution")),
      ("H2OGLM", Seq("family")),
      ("H2OGAM", Seq("family")),
      ("H2ODeepLearning", Seq("distribution")),
      ("H2ORuleFit", Seq("distribution")))

    for ((parameterEntityName, parametersToCheck) <- algorithms)
      yield ProblemSpecificAlgorithmSubstitutionContext(
        null,
        parameterEntityName,
        null,
        "ai.h2o.sparkling.ml.algos",
        parametersToCheck)
  }

  override def modelOutputConfiguration: Seq[ModelOutputSubstitutionContext] = {
    val modelOutputs = Seq[(String, Class[_])](
      ("H2OXGBoostModelOutputs", classOf[XGBoostModelOutputV3]),
      ("H2OGBMModelOutputs", classOf[GBMModelOutputV3]),
      ("H2ODRFModelOutputs", classOf[DRFModelOutputV3]),
      ("H2OGLMModelOutputs", classOf[GLMModelOutputV3]),
      ("H2OGAMModelOutputs", classOf[GAMModelOutputV3]),
      ("H2ODeepLearningModelOutputs", classOf[DeepLearningModelOutputV3]),
      ("H2ORuleFitModelOutputs", classOf[RuleFitModelOutputV3]),
      ("H2OKMeansModelOutputs", classOf[KMeansModelOutputV3]),
      ("H2OCoxPHModelOutputs", classOf[CoxPHModelOutputV3]),
      ("H2OIsolationForestModelOutputs", classOf[IsolationForestModelOutputV3]),
      ("H2OExtendedIsolationForestModelOutputs", classOf[ExtendedIsolationForestModelOutputV3]))

    for ((outputEntityName, h2oParametersClass: Class[_]) <- modelOutputs)
      yield ModelOutputSubstitutionContext(
        "ai.h2o.sparkling.ml.outputs",
        outputEntityName,
        h2oParametersClass,
        Seq.empty)
  }
}
