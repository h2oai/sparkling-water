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

package ai.h2o.sparkling.api.generation

import java.io.{File, PrintWriter}
import java.util

import ai.h2o.automl.AutoMLBuildSpec._
import ai.h2o.sparkling.api.generation.common._
import ai.h2o.sparkling.utils.ScalaUtils._
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.schemas._
import hex.tree.gbm.GBMModel.GBMParameters
import hex.glm.GLMModel.GLMParameters
import hex.grid.HyperSpaceSearchCriteria
import hex.grid.HyperSpaceSearchCriteria._
import hex.kmeans.KMeansModel.KMeansParameters
import hex.schemas.HyperSpaceSearchCriteriaV99.{CartesianSearchCriteriaV99, RandomDiscreteValueSearchCriteriaV99}
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import water.automl.api.schemas3.AutoMLBuildSpecV99._

object Runner {

  val defaultValuesOfCommonParameters = Map(
    "featuresCols" -> Array.empty[String],
    "predictionCol" -> "prediction",
    "detailedPredictionCol" -> "detailed_prediction",
    "withDetailedPredictionCol" -> false,
    "convertUnknownCategoricalLevelsToNa" -> false,
    "convertInvalidNumbersToNa" -> false,
    "namedMojoOutputColumns" -> true,
    "withContributions" -> false,
    "splitRatio" -> 1.0,
    "columnsToCategorical" -> Array.empty[String],
    "withLeafNodeAssignments" -> false)

  val ignoredCols = ExplicitField("ignored_columns", "HasIgnoredCols", null)

  private def parametersConfiguration: Seq[ParameterSubstitutionContext] = {
    val monotonicity =
      ExplicitField("monotone_constraints", "HasMonotoneConstraints", new util.HashMap[String, Double]())
    val calibrationDataFrame = ExplicitField("calibration_frame", "HasCalibrationDataFrame", null)
    val plugValues = ExplicitField("plug_values", "HasPlugValues", null)
    val betaConstraints = ExplicitField("beta_constraints", "HasBetaConstraints", null)
    val userPoints = ExplicitField("user_points", "HasUserPoints", null)
    val randomCols = ExplicitField("random_columns", "HasRandomCols", null)

    val xgboostFields = Seq(monotonicity, calibrationDataFrame, ignoredCols)
    val glmFields = Seq(randomCols, ignoredCols, plugValues, betaConstraints)
    val gbmFields = Seq(monotonicity, calibrationDataFrame, ignoredCols)
    val kmeansFields = Seq(userPoints, ignoredCols)

    val deepLearningFields = Seq(
      ExplicitField("initial_biases", "HasInitialBiases", null),
      ExplicitField("initial_weights", "HasInitialWeights", null),
      ignoredCols)
    type DeepLearningParametersV3 = DeepLearningV3.DeepLearningParametersV3

    val explicitDefaultValues =
      Map[String, Any]("max_w2" -> 3.402823e38f, "response_column" -> "label", "model_id" -> null)

    val algorithmParameters = Seq[(String, Class[_], Class[_], Seq[ExplicitField])](
      ("H2OXGBoostParams", classOf[XGBoostV3.XGBoostParametersV3], classOf[XGBoostParameters], xgboostFields),
      ("H2OGBMParams", classOf[GBMV3.GBMParametersV3], classOf[GBMParameters], gbmFields),
      ("H2ODRFParams", classOf[DRFV3.DRFParametersV3], classOf[DRFParameters], Seq(calibrationDataFrame, ignoredCols)),
      ("H2OGLMParams", classOf[GLMV3.GLMParametersV3], classOf[GLMParameters], glmFields),
      ("H2ODeepLearningParams", classOf[DeepLearningParametersV3], classOf[DeepLearningParameters], deepLearningFields),
      ("H2OKMeansParams", classOf[KMeansV3.KMeansParametersV3], classOf[KMeansParameters], kmeansFields))

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], explicitFields) <- algorithmParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        IgnoredParameters.all ++
          (if (entityName == "H2OKMeansParams") Seq("response_column", "offset_column") else Seq.empty),
        explicitFields,
        explicitDefaultValues,
        typeExceptions = TypeExceptions.all(),
        defaultValueSource = DefaultValueSource.Field,
        defaultValuesOfCommonParameters = defaultValuesOfCommonParameters,
        generateParamTag = true)

  }

  private def algorithmConfiguration: Seq[AlgorithmSubstitutionContext] = {

    val algorithms = Seq[(String, Class[_], String, Seq[String])](
      ("H2OXGBoost", classOf[XGBoostParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2OGBM", classOf[GBMParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2ODRF", classOf[DRFParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2OGLM", classOf[GLMParameters], "H2OSupervisedAlgorithm", Seq.empty),
      ("H2ODeepLearning", classOf[DeepLearningParameters], "H2OSupervisedAlgorithm", Seq.empty),
      ("H2OKMeans", classOf[KMeansParameters], "H2OUnsupervisedAlgorithm", Seq("H2OKMeansExtras")))

    for ((entityName, h2oParametersClass: Class[_], algorithmType, extraParents) <- algorithms)
      yield AlgorithmSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.algos",
        entityName,
        h2oParametersClass,
        algorithmType,
        extraParents)
  }

  private def problemSpecificAlgorithmConfiguration: Seq[ProblemSpecificAlgorithmSubstitutionContext] = {

    val algorithms = Seq[(String, Seq[String])](
      ("H2OXGBoost", Seq("distribution")),
      ("H2OGBM", Seq("distribution")),
      ("H2ODRF", Seq("distribution")),
      ("H2OGLM", Seq("distribution", "family")),
      ("H2ODeepLearning", Seq("distribution")))

    for ((parameterEntityName, parametersToCheck) <- algorithms)
      yield ProblemSpecificAlgorithmSubstitutionContext(
        null,
        parameterEntityName,
        null,
        "ai.h2o.sparkling.ml.algos",
        parametersToCheck)
  }

  private def autoMLParameterConfiguration: Seq[ParameterSubstitutionContext] = {
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
        explicitDefaultValues =
          Map("include_algos" -> ai.h2o.automl.Algo.values().map(_.name()), "response_column" -> "label"),
        defaultValueFieldPrefix = "",
        typeExceptions = AutoMLTypeExceptions.all(),
        defaultValueSource = source,
        defaultValuesOfCommonParameters = defaultValuesOfCommonParameters ++
          Map("monotoneConstraints" -> new util.HashMap[String, Double](), "ignoredCols" -> ignoredCols.defaultValue),
        generateParamTag = false)
  }

  private def autoMLAlgorithmContext: AlgorithmSubstitutionContext = {
    AlgorithmSubstitutionContext(
      namespace = "ai.h2o.sparkling.ml.algos",
      "H2OAutoML",
      null,
      "H2OSupervisedAlgorithm",
      Seq("H2OAutoMLExtras"))
  }

  private def problemSpecificAutoMLAlgorithmContext: ProblemSpecificAlgorithmSubstitutionContext = {
    ProblemSpecificAlgorithmSubstitutionContext(null, "H2OAutoML", null, "ai.h2o.sparkling.ml.algos", Seq.empty)
  }

  private def gridSearchParameterConfiguration: Seq[ParameterSubstitutionContext] = {
    class DummySearchCriteria extends HyperSpaceSearchCriteriaV99[HyperSpaceSearchCriteria, DummySearchCriteria]

    val gridSearchParameters = Seq[(String, Class[_], Class[_], Seq[String])](
      (
        "H2OGridSearchRandomDiscreteCriteriaParams",
        classOf[RandomDiscreteValueSearchCriteriaV99],
        classOf[RandomDiscreteValueSearchCriteria],
        Seq("strategy")),
      (
        "H2OGridSearchCartesianCriteriaParams",
        classOf[CartesianSearchCriteriaV99],
        classOf[CartesianSearchCriteria],
        Seq("strategy")),
      ("H2OGridSearchCommonCriteriaParams", classOf[DummySearchCriteria], classOf[CartesianSearchCriteria], Seq.empty))

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], extraIgnoredParameters) <- gridSearchParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        ignoredParameters = Seq("__meta") ++ extraIgnoredParameters,
        explicitFields = Seq.empty,
        explicitDefaultValues = Map.empty,
        typeExceptions = Map.empty,
        defaultValueSource = DefaultValueSource.Getter,
        defaultValuesOfCommonParameters = Map(
          "algo" -> null,
          "hyperParameters" -> new util.HashMap[String, AnyRef](),
          "selectBestModelBy" -> "AUTO",
          "parallelism" -> 1),
        generateParamTag = false)
  }

  private def gridSearchAlgorithmContext: AlgorithmSubstitutionContext = {
    AlgorithmSubstitutionContext(
      namespace = "ai.h2o.sparkling.ml.algos",
      "H2OGridSearch",
      null,
      "H2OAlgorithm",
      Seq("H2OGridSearchExtras"))
  }

  private def writeResultToFile(
      content: String,
      substitutionContext: SubstitutionContextBase,
      languageExtension: String,
      destinationDir: String) = {
    val fileName = substitutionContext.entityName
    val namespacePath = substitutionContext.namespace.replace('.', '/')
    val destinationDirWithNamespace = new File(destinationDir, namespacePath)
    destinationDirWithNamespace.mkdirs()
    val destinationFile = new File(destinationDirWithNamespace, s"$fileName.$languageExtension")
    withResource(new PrintWriter(destinationFile)) { outputStream =>
      outputStream.print(content)
    }
  }

  private val algorithmTemplates = Map("scala" -> scala.AlgorithmTemplate, "py" -> python.AlgorithmTemplate)

  private val problemSpecificAlgorithmTemplates =
    Map("scala" -> scala.ProblemSpecificAlgorithmTemplate, "py" -> python.ProblemSpecificAlgorithmTemplate)

  private val parameterTemplates = Map("scala" -> scala.ParametersTemplate, "py" -> python.ParametersTemplate)

  def main(args: Array[String]): Unit = {
    val languageExtension = args(0)
    val destinationDir = args(1)

    for (substitutionContext <- parametersConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    for ((algorithmContext, parameterContext) <- algorithmConfiguration.zip(parametersConfiguration)) {
      val content = algorithmTemplates(languageExtension)(algorithmContext, Seq(parameterContext))
      writeResultToFile(content, algorithmContext, languageExtension, destinationDir)
    }

    val parametersConfigurationSequences = parametersConfiguration.map(Seq(_))
    val specificAlgorithmCombinations = problemSpecificAlgorithmConfiguration.zip(parametersConfigurationSequences) :+
      (problemSpecificAutoMLAlgorithmContext, autoMLParameterConfiguration)

    for ((algorithmContext, parameterContexts) <- specificAlgorithmCombinations) {
      val classificationAlgorithmContext = algorithmContext.copy(
        entityName = algorithmContext.parentEntityName + "Classifier",
        namespace = algorithmContext.parentNamespace + ".classification")
      val content = problemSpecificAlgorithmTemplates(languageExtension)(
        "classification",
        classificationAlgorithmContext,
        parameterContexts)
      writeResultToFile(content, classificationAlgorithmContext, languageExtension, destinationDir)
    }

    for ((algorithmContext, parameterContexts) <- specificAlgorithmCombinations) {
      val regressionAlgorithmContext = algorithmContext.copy(
        entityName = algorithmContext.parentEntityName + "Regressor",
        namespace = algorithmContext.parentNamespace + ".regression")
      val content = problemSpecificAlgorithmTemplates(languageExtension)(
        "regression",
        regressionAlgorithmContext,
        parameterContexts)
      writeResultToFile(content, regressionAlgorithmContext, languageExtension, destinationDir)
    }

    for (substitutionContext <- autoMLParameterConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    if (languageExtension != "scala") {
      val content = algorithmTemplates(languageExtension)(autoMLAlgorithmContext, autoMLParameterConfiguration)
      writeResultToFile(content, autoMLAlgorithmContext, languageExtension, destinationDir)
    }

    for (substitutionContext <- gridSearchParameterConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    if (languageExtension != "scala") {
      val content = algorithmTemplates(languageExtension)(gridSearchAlgorithmContext, gridSearchParameterConfiguration)
      writeResultToFile(content, gridSearchAlgorithmContext, languageExtension, destinationDir)
    }
  }
}
