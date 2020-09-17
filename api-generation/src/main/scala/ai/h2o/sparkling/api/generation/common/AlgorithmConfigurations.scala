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

import java.util

import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.gam.GAMModel.GAMParameters
import hex.glm.GLMModel.GLMParameters
import hex.glrm.GLRMModel.GLRMParameters
import hex.kmeans.KMeansModel.KMeansParameters
import hex.pca.PCAModel.PCAParameters
import hex.schemas.{DRFV3, DeepLearningV3, GAMV3, GBMV3, GLMV3, GLRMV3, IsolationForestV3, KMeansV3, PCAV3, XGBoostV3}
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.isofor.IsolationForestModel.IsolationForestParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters

trait AlgorithmConfigurations {

  val defaultValuesOfCommonParameters = Map(
    "featuresCols" -> Array.empty[String],
    "predictionCol" -> "prediction",
    "detailedPredictionCol" -> "detailed_prediction",
    "withDetailedPredictionCol" -> true,
    "convertUnknownCategoricalLevelsToNa" -> false,
    "convertInvalidNumbersToNa" -> false,
    "namedMojoOutputColumns" -> true,
    "withContributions" -> false,
    "validationDataFrame" -> null,
    "splitRatio" -> 1.0,
    "columnsToCategorical" -> Array.empty[String],
    "withLeafNodeAssignments" -> false,
    "withStageResults" -> false,
    "withReconstructedData" -> false)

  val ignoredCols = ExplicitField("ignored_columns", "HasIgnoredCols", null, None, Some("HasIgnoredColsOnMOJO"))

  def parametersConfiguration: Seq[ParameterSubstitutionContext] = {
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
    val userX = ExplicitField("user_x", "HasUserX", null)
    val userY = ExplicitField("user_y", "HasUserY", null)
    val lossByColNames = ExplicitField("loss_by_col_idx", "HasLossByColNames", null, Some("lossByColNames"))
    val gamCols = ExplicitField("gam_columns", "HasGamCols", null, None, Some("HasGamColsOnMOJO"))
    val validationLabelCol = ExplicitField("validation_response_column", "HasValidationLabelCol", "label")
    val distribution = ExplicitField("distribution", "HasDistribution", "distribution")
    val interactionPairs = ExplicitField("interaction_pairs", "HasInteractionPairs", null)

    val xgboostFields = Seq(monotonicity, calibrationDataFrame, ignoredCols)
    val glmFields = Seq(randomCols, ignoredCols, plugValues, betaConstraints, interactionPairs, distribution)
    val gamFields = Seq(ignoredCols, betaConstraints, gamCols, distribution)
    val gbmFields = Seq(monotonicity, calibrationDataFrame, ignoredCols)
    val kmeansFields = Seq(userPoints, ignoredCols)
    val pcaFields = Seq(ignoredCols)
    val ifFields = Seq(calibrationDataFrame, validationLabelCol)

    val deepLearningFields = Seq(
      ExplicitField("initial_biases", "HasInitialBiases", null),
      ExplicitField("initial_weights", "HasInitialWeights", null),
      ignoredCols)
    type DeepLearningParametersV3 = DeepLearningV3.DeepLearningParametersV3
    type IsolationForestParametersV3 = IsolationForestV3.IsolationForestParametersV3

    val explicitDefaultValues = Map[String, Any](
      "max_w2" -> 3.402823e38f,
      "response_column" -> "label",
      "model_id" -> null,
      "pca_impl" -> new PCAParameters()._pca_implementation)

    val algorithmParameters = Seq[(String, Class[_], Class[_], Seq[ExplicitField])](
      ("H2OXGBoostParams", classOf[XGBoostV3.XGBoostParametersV3], classOf[XGBoostParameters], xgboostFields),
      ("H2OGBMParams", classOf[GBMV3.GBMParametersV3], classOf[GBMParameters], gbmFields),
      ("H2ODRFParams", classOf[DRFV3.DRFParametersV3], classOf[DRFParameters], Seq(calibrationDataFrame, ignoredCols)),
      ("H2OGLMParams", classOf[GLMV3.GLMParametersV3], classOf[GLMParameters], glmFields),
      ("H2OGAMParams", classOf[GAMV3.GAMParametersV3], classOf[GAMParameters], gamFields),
      ("H2ODeepLearningParams", classOf[DeepLearningParametersV3], classOf[DeepLearningParameters], deepLearningFields),
      ("H2OKMeansParams", classOf[KMeansV3.KMeansParametersV3], classOf[KMeansParameters], kmeansFields),
      ("H2OGLRMParams", classOf[GLRMV3.GLRMParametersV3], classOf[GLRMParameters], Seq(userX, userY, lossByColNames)),
      ("H2OPCAParams", classOf[PCAV3.PCAParametersV3], classOf[PCAParameters], pcaFields),
      ("H2OIsolationForestParams", classOf[IsolationForestParametersV3], classOf[IsolationForestParameters], ifFields))

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], explicitFields) <- algorithmParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        IgnoredParameters.all(entityName.replace("Params", "")),
        explicitFields,
        explicitDefaultValues,
        typeExceptions = Map.empty,
        defaultValueSource = DefaultValueSource.Field,
        defaultValuesOfCommonParameters = defaultValuesOfCommonParameters,
        generateParamTag = true)
  }

  private def isUnsupervised(entityName: String): Boolean = {
    Array("H2OGLRMParams", "H2OKMeansParams", "H2OPCAParams", "H2OIsolationForestParams").contains(entityName)
  }

  def algorithmConfiguration: Seq[AlgorithmSubstitutionContext] = {

    val algorithms = Seq[(String, Class[_], String, Seq[String])](
      ("H2OXGBoost", classOf[XGBoostParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2OGBM", classOf[GBMParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2ODRF", classOf[DRFParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2OGLM", classOf[GLMParameters], "H2OSupervisedAlgorithm", Seq.empty),
      ("H2OGAM", classOf[GAMParameters], "H2OSupervisedAlgorithm", Seq.empty),
      ("H2ODeepLearning", classOf[DeepLearningParameters], "H2OSupervisedAlgorithm", Seq.empty),
      ("H2OKMeans", classOf[KMeansParameters], "H2OUnsupervisedAlgorithm", Seq("H2OKMeansExtras")),
      ("H2OGLRM", classOf[GLRMParameters], "H2OUnsupervisedAlgorithm", Seq.empty),
      ("H2OPCA", classOf[PCAParameters], "H2OUnsupervisedAlgorithm", Seq.empty),
      ("H2OIsolationForest", classOf[IsolationForestParameters], "H2OTreeBasedUnsupervisedAlgorithm", Seq.empty))

    for ((entityName, h2oParametersClass: Class[_], algorithmType, extraParents) <- algorithms)
      yield AlgorithmSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.algos",
        entityName,
        h2oParametersClass,
        algorithmType,
        extraParents)
  }

  def problemSpecificAlgorithmConfiguration: Seq[ProblemSpecificAlgorithmSubstitutionContext] = {

    val algorithms = Seq[(String, Seq[String])](
      ("H2OXGBoost", Seq("distribution")),
      ("H2OGBM", Seq("distribution")),
      ("H2ODRF", Seq("distribution")),
      ("H2OGLM", Seq("distribution", "family")),
      ("H2OGAM", Seq("distribution", "family")),
      ("H2ODeepLearning", Seq("distribution")))

    for ((parameterEntityName, parametersToCheck) <- algorithms)
      yield ProblemSpecificAlgorithmSubstitutionContext(
        null,
        parameterEntityName,
        null,
        "ai.h2o.sparkling.ml.algos",
        parametersToCheck)
  }
}
