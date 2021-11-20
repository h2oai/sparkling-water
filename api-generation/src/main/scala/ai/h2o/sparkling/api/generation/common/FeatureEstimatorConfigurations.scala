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

import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.glrm.GLRMModel.GLRMParameters
import hex.pca.PCAModel.PCAParameters
import hex.schemas.DeepLearningModelV3.DeepLearningModelOutputV3
import hex.schemas.GLRMModelV3.GLRMModelOutputV3
import hex.schemas.PCAModelV3.PCAModelOutputV3
import hex.schemas.Word2VecModelV3.Word2VecModelOutputV3
import hex.schemas.{DeepLearningV3, GLRMV3, PCAV3, Word2VecV3}
import hex.word2vec.Word2VecModel.Word2VecParameters

trait FeatureEstimatorConfigurations extends ConfigurationsBase {

  override def parametersConfiguration: Seq[ParameterSubstitutionContext] = super.parametersConfiguration ++ {

    val dlFields = Seq(
      ExplicitField("initial_biases", "HasInitialBiases", null),
      ExplicitField("initial_weights", "HasInitialWeights", null),
      ignoredCols)
    val pcaFields = Seq(ignoredCols)
    val glrmFields = Seq(
      ExplicitField("user_x", "HasUserX", null),
      ExplicitField("user_y", "HasUserY", null),
      ExplicitField("loss_by_col_idx", "HasLossByColNames", null, Some("lossByColNames")))
    val word2VecFields = Seq(ExplicitField("pre_trained", "HasPreTrained", null))

    type DLParamsV3 = DeepLearningV3.DeepLearningParametersV3
    type PCAParamsV3 = PCAV3.PCAParametersV3
    type GLRMParamsV3 = GLRMV3.GLRMParametersV3
    type Word2VecParamsV3 = Word2VecV3.Word2VecParametersV3

    val explicitDefaultValues = Map[String, Any](
      "max_w2" -> 3.402823e38f,
      "model_id" -> null,
      "pca_impl" -> new PCAParameters()._pca_implementation)

    val noDeprecation = Seq.empty

    val aeDefaultValues = Map(
      "inputCols" -> Array.empty[String],
      "outputCol" -> "AutoEncoder__output",
      "originalCol" -> "AutoEncoder__original",
      "withOriginalCol" -> false,
      "mseCol" -> "AutoEncoder__mse",
      "withMSECol" -> false)
    val pcaDefaultValues = Map("inputCols" -> Array.empty[String], "outputCol" -> "PCA__output")
    val glrmDefaultValues = Map(
      "inputCols" -> Array.empty[String],
      "outputCol" -> "GLRM__output",
      "reconstructedCol" -> "GLRM__reconstructed",
      "withReconstructedCol" -> false,
      "maxScoringIterations" -> 100)
    val word2VecDefaultValues = Map("inputCol" -> null, "outputCol" -> "Word2Vec__output")

    val algorithmParameters =
      Seq[(String, Class[_], Class[_], Seq[ExplicitField], Seq[DeprecatedField], Map[String, Any])](
        (
          "H2OAutoEncoderParams",
          classOf[DLParamsV3],
          classOf[DeepLearningParameters],
          dlFields,
          noDeprecation,
          aeDefaultValues),
        ("H2OPCAParams", classOf[PCAParamsV3], classOf[PCAParameters], pcaFields, noDeprecation, pcaDefaultValues),
        ("H2OGLRMParams", classOf[GLRMParamsV3], classOf[GLRMParameters], glrmFields, noDeprecation, glrmDefaultValues),
        (
          "Word2VecParamsV3",
          classOf[Word2VecParamsV3],
          classOf[Word2VecParameters],
          word2VecFields,
          noDeprecation,
          word2VecDefaultValues))

    for ((
           entityName,
           h2oSchemaClass: Class[_],
           h2oParameterClass: Class[_],
           explicitFields,
           deprecatedFields,
           extraDefaultValues) <- algorithmParameters)
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
        defaultValuesOfCommonParameters = defaultValuesOfCommonParameters ++ extraDefaultValues,
        generateParamTag = true)
  }

  override def algorithmConfiguration: Seq[AlgorithmSubstitutionContext] = super.algorithmConfiguration ++ {

    def none = Seq.empty[String]
    val algorithms = Seq[(String, Class[_], String, Option[String])](
      (
        "H2OAutoEncoder",
        classOf[DeepLearningParameters],
        "H2OAutoEncoderBase",

        Some("H2OAutoEncoderMetrics")),
      (
        "H2OPCA",
        classOf[PCAParameters],
        "H2ODimReductionEstimator",

        Some("H2OPCAMetrics")),
      ("H2OGLRM", classOf[GLRMParameters], "H2OGLRMBase", Some("H2OGLRMMetrics")),
      ("H2OWord2Vec", classOf[Word2VecParameters], "H2OWord2VecBase", Some("H2OCommonMetrics")))

    for ((entityName, h2oParametersClass: Class[_], algorithmType, mojoBaseClasses, metricsClass) <- algorithms)
      yield AlgorithmSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.features",
        entityName,
        h2oParametersClass,
        algorithmType,
        specificMetricsClass = metricsClass,
        extraInheritedEntitiesOnMOJO = mojoBaseClasses)
  }

  override def modelOutputConfiguration: Seq[ModelOutputSubstitutionContext] = super.modelOutputConfiguration ++ {
    val modelOutputs = Seq[(String, Class[_])](
      ("H2OAutoEncoderModelOutputs", classOf[DeepLearningModelOutputV3]),
      ("H2OPCAModelOutputs", classOf[PCAModelOutputV3]),
      ("H2OGLRMModelOutputs", classOf[GLRMModelOutputV3]),
      ("H2OWord2VecModelOutputs", classOf[Word2VecModelOutputV3]))

    for ((outputEntityName, h2oParametersClass: Class[_]) <- modelOutputs)
      yield ModelOutputSubstitutionContext(
        "ai.h2o.sparkling.ml.outputs",
        outputEntityName,
        h2oParametersClass,
        Seq.empty)
  }
}
