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

import ai.h2o.automl.AutoMLBuildSpec._
import ai.h2o.sparkling.api.generation.common._
import ai.h2o.sparkling.api.generation.scala.{AlgorithmTemplate, ParametersTemplate}
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

  private def parametersConfiguration: Seq[ParameterSubstitutionContext] = {
    val monotonicity = ExplicitField("monotone_constraints", "HasMonotoneConstraints")
    val userPoints = ExplicitField("user_points", "HasUserPoints")
    type DeepLearningParametersV3 = DeepLearningV3.DeepLearningParametersV3

    val explicitDefaultValues =
      Map("max_w2" -> "java.lang.Float.MAX_VALUE", "response_column" -> """"label"""", "model_id" -> "null")

    val algorithmParameters = Seq[(String, Class[_], Class[_], Seq[ExplicitField])](
      ("H2OXGBoostParams", classOf[XGBoostV3.XGBoostParametersV3], classOf[XGBoostParameters], Seq(monotonicity)),
      ("H2OGBMParams", classOf[GBMV3.GBMParametersV3], classOf[GBMParameters], Seq(monotonicity)),
      ("H2ODRFParams", classOf[DRFV3.DRFParametersV3], classOf[DRFParameters], Seq.empty),
      ("H2OGLMParams", classOf[GLMV3.GLMParametersV3], classOf[GLMParameters], Seq.empty),
      ("H2ODeepLearningParams", classOf[DeepLearningParametersV3], classOf[DeepLearningParameters], Seq.empty),
      ("H2OKMeansParams", classOf[KMeansV3.KMeansParametersV3], classOf[KMeansParameters], Seq(userPoints)))

    algorithmParameters.map {
      case (entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], explicitFields) =>
        ParameterSubstitutionContext(
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
          generateParamTag = true)
    }
  }

  private def algorithmConfiguration: Seq[AlgorithmSubstitutionContext] = {

    val algorithms = Seq[(String, Class[_], String, Seq[String])](
      ("H2OXGBoost", classOf[XGBoostParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2OGBM", classOf[GBMParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2ODRF", classOf[DRFParameters], "H2OTreeBasedSupervisedAlgorithm", Seq.empty),
      ("H2OGLM", classOf[GLMParameters], "H2OSupervisedAlgorithm", Seq.empty),
      ("H2ODeepLearning", classOf[DeepLearningParameters], "H2OSupervisedAlgorithm", Seq.empty),
      ("H2OKMeans", classOf[KMeansParameters], "H2OUnsupervisedAlgorithm", Seq("H2OKMeansExtras")))

    algorithms.map {
      case (entityName, h2oParametersClass: Class[_], algorithmType, extraParents) =>
        AlgorithmSubstitutionContext(
          namespace = "ai.h2o.sparkling.ml.algos",
          entityName,
          h2oParametersClass,
          algorithmType,
          extraParents)
    }
  }

  private def autoMLParameterConfiguration: Seq[ParameterSubstitutionContext] = {
    import DefaultValueSource._

    val autoMLParameters = Seq[(String, Class[_], Class[_], DefaultValueSource)](
      ("H2OAutoMLBuildControlParams", classOf[AutoMLBuildControlV99], classOf[AutoMLBuildControl], Field),
      ("H2OAutoMLInputParams", classOf[AutoMLInputV99], classOf[AutoMLInput], Field),
      ("H2OAutoMLStoppingCriteriaParams", classOf[AutoMLStoppingCriteriaV99], classOf[AutoMLStoppingCriteria], Getter),
      ("H2OAutoMLBuildModelsParams", classOf[AutoMLBuildModelsV99], classOf[AutoMLBuildModels], Field))

    autoMLParameters.map {
      case (entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], source: DefaultValueSource) =>
        ParameterSubstitutionContext(
          namespace = "ai.h2o.sparkling.ml.params",
          entityName,
          h2oSchemaClass,
          h2oParameterClass,
          AutoMLIgnoredParameters.all,
          explicitFields = Seq.empty,
          explicitDefaultValues =
            Map("include_algos" -> "ai.h2o.automl.Algo.values().map(_.name())", "response_column" -> """"label""""),
          defaultValueFieldPrefix = "",
          typeExceptions = AutoMLTypeExceptions.all(),
          defaultValueSource = source,
          generateParamTag = false)
    }
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

    gridSearchParameters.map {
      case (entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], extraIgnoredParameters) =>
        ParameterSubstitutionContext(
          namespace = "ai.h2o.sparkling.ml.params",
          entityName,
          h2oSchemaClass,
          h2oParameterClass,
          ignoredParameters = Seq("__meta") ++ extraIgnoredParameters,
          explicitFields = Seq.empty,
          explicitDefaultValues = Map.empty,
          typeExceptions = Map.empty,
          defaultValueSource = DefaultValueSource.Getter,
          generateParamTag = false)
    }
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

  private val parameterTemplates = Map("scala" -> scala.ParametersTemplate, "py" -> python.ParametersTemplate)

  def main(args: Array[String]): Unit = {
    val languageExtension = args(0)
    val destinationDir = args(1)

    for (substitutionContext <- parametersConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    for (substitutionContext <- algorithmConfiguration) {
      val content = algorithmTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    for (substitutionContext <- autoMLParameterConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    for (substitutionContext <- gridSearchParameterConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }
  }
}
