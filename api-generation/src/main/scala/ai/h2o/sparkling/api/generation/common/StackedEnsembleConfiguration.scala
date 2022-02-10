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

import hex.Model.Parameters.FoldAssignmentScheme
import hex.ensemble.StackedEnsembleModel.StackedEnsembleParameters
import hex.schemas.StackedEnsembleModelV99.StackedEnsembleModelOutputV99
import hex.schemas.StackedEnsembleV99.StackedEnsembleParametersV99

class StackedEnsembleConfiguration extends SingleAlgorithmConfiguration {

  override def parametersConfiguration: Seq[ParameterSubstitutionContext] = {

    val stackedEnsembleParameters = Seq[(String, Class[_], Class[_])](
      ("H2OStackedEnsembleParams", classOf[StackedEnsembleParametersV99], classOf[StackedEnsembleParameters]))

    val blendingFrame = ExplicitField("blending_frame", "HasBlendingDataFrame", null, Some("blendingDataFrame"), None)
    val explicitDefaultValues =
      Map[String, Any]("model_id" -> null, "metalearner_fold_assignment" -> FoldAssignmentScheme.AUTO)

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_]) <- stackedEnsembleParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        ignoredParameters = Seq("__meta", "base_models", "training_frame", "validation_frame"),
        explicitFields = Seq(blendingFrame),
        deprecatedFields = Seq.empty,
        explicitDefaultValues, // = Map.empty,
        typeExceptions = Map.empty,
        defaultValueSource = DefaultValueSource.Field,
        defaultValuesOfCommonParameters = Map(),
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
