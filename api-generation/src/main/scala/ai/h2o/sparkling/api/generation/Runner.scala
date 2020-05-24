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
import hex.kmeans.KMeansModel.KMeansParameters
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import water.automl.api.schemas3.AutoMLBuildSpecV99._

object Runner {

  private val explicitDefaultValues = Map("max_w2" -> "java.lang.Float.MAX_VALUE")

  private def parametersConfiguration: Seq[ParameterSubstitutionContext] = {
    val monotonicity = ExplicitField("monotone_constraints", "HasMonotoneConstraints")
    val userPoints = ExplicitField("user_points", "HasUserPoints")
    type DeepLearningParametersV3 = DeepLearningV3.DeepLearningParametersV3

    val algorithmParameters = Seq[(String, Class[_], Class[_], Seq[ExplicitField])](
      ("H2OXGBoostParams", classOf[XGBoostV3.XGBoostParametersV3], classOf[XGBoostParameters], Seq(monotonicity)),
      ("H2OGBMParams", classOf[GBMV3.GBMParametersV3], classOf[GBMParameters], Seq(monotonicity)),
      ("H2ODRFParams", classOf[DRFV3.DRFParametersV3], classOf[DRFParameters], Seq.empty),
      ("H2OGLMParams", classOf[GLMV3.GLMParametersV3], classOf[GLMParameters], Seq.empty),
      ("H2ODeepLearningParams", classOf[DeepLearningParametersV3], classOf[DeepLearningParameters], Seq.empty),
      ("H2OKMeansParams", classOf[KMeansV3.KMeansParametersV3], classOf[KMeansParameters], Seq(userPoints)))

    algorithmParameters.map {
      case (entityName, h2oSchemaClass: Class[_], h2oParametersClass: Class[_], explicitFields) =>
        ParameterSubstitutionContext(
          namespace = "ai.h2o.sparkling.ml.params",
          entityName,
          h2oSchemaClass,
          h2oParametersClass,
          IgnoredParameters.all,
          explicitFields,
          explicitDefaultValues,
          typeExceptions = Map.empty,
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
      case (entityName, h2oSchemaClass: Class[_], h2oParametersClass: Class[_], source: DefaultValueSource) =>
        ParameterSubstitutionContext(
          namespace = "ai.h2o.sparkling.ml.params",
          entityName,
          h2oSchemaClass,
          h2oParametersClass,
          AutoMLIgnoredParameters.all,
          explicitFields = Seq.empty,
          explicitDefaultValues = Map("include_algos" -> "ai.h2o.automl.Algo.values().map(_.name())"),
          defaultValueFieldPrefix = "",
          typeExceptions = AutoMLTypeExceptions.all(),
          defaultValueSource = source,
          generateParamTag = false)
    }
  }

  private def writeResultToFile(
      content: String,
      substitutionContext: SubstitutionContextBase,
      language: String,
      destinationDir: String) = {
    val fileName = substitutionContext.entityName
    val namespacePath = substitutionContext.namespace.replace('.', '/')
    val destinationDirWithNamespace = new File(destinationDir, namespacePath)
    destinationDirWithNamespace.mkdirs()
    val destinationFile = new File(destinationDirWithNamespace, s"$fileName.$language")
    withResource(new PrintWriter(destinationFile)) { outputStream =>
      outputStream.print(content)
    }
  }

  def main(args: Array[String]): Unit = {
    val language = args(0)
    val destinationDir = args(1)

    for (substitutionContext <- parametersConfiguration) {
      val content = ParametersTemplate(substitutionContext)
      writeResultToFile(content, substitutionContext, language, destinationDir)
    }

    for (substitutionContext <- algorithmConfiguration) {
      val content = AlgorithmTemplate(substitutionContext)
      writeResultToFile(content, substitutionContext, language, destinationDir)
    }

    for (substitutionContext <- autoMLParameterConfiguration) {
      val content = ParametersTemplate(substitutionContext)
      writeResultToFile(content, substitutionContext, language, destinationDir)
    }
  }
}
