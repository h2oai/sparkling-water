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

package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2OAlgorithmMOJOParams
import com.google.gson.JsonObject
import hex.ModelCategory
import hex.genmodel.{GenModel, MojoModel, PredictContributionsFactory}
import hex.genmodel.algos.tree.{SharedTreeMojoModel, TreeBackedMojoModel}
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class H2OAlgorithmMOJOModel(override val uid: String)
  extends H2OMOJOModel
  with H2OMOJOPrediction
  with H2OAlgorithmMOJOParams {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val baseDf = applyPredictionUdf(dataset, _ => getPredictionUDF())

    baseDf.withColumn(getPredictionCol(), extractPredictionColContent())
  }

  override protected def inputColumnNames: Array[String] = getFeaturesCols()

  override protected def outputColumnName: String = getDetailedPredictionCol()

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // Here we should check validity of input schema however
    // in theory user can pass invalid schema with missing columns
    // and model will be able to still provide a prediction
    StructType(schema.fields ++ getDetailedPredictionColSchema() ++ getPredictionColSchema())
  }

  private[sparkling] override def setParameters(
      mojoModel: MojoModel,
      modelJson: JsonObject,
      settings: H2OMOJOSettings): Unit = {
    super.setParameters(mojoModel, modelJson, settings)
    set(this.featuresCols -> mojoModel.features())
    set(this.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    set(this.predictionCol -> settings.predictionCol)
    set(this.detailedPredictionCol -> settings.detailedPredictionCol)
    set(this.withContributions -> settings.withContributions)
    set(this.withLeafNodeAssignments -> settings.withLeafNodeAssignments)
    set(this.withStageResults -> settings.withStageResults)
  }

  private[sparkling] override def getEasyPredictModelWrapperConfigurationInitializers()
      : Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config] = {
    val superInitializers = super.getEasyPredictModelWrapperConfigurationInitializers()
    val mojoModel = unwrapMojoModel()
    val enableContributions = this.getWithContributions() && canGenerateContributions(mojoModel)
    val enableLeafNodeAssignments = this.getWithLeafNodeAssignments() && canGenerateLeafNodeAssignments(mojoModel)
    val enableStagedProbabilities = this.getWithStageResults() && canGenerateStageResults(mojoModel)

    superInitializers ++
      (((config: EasyPredictModelWrapper.Config) => config.setEnableContributions(enableContributions)) ::
        ((config: EasyPredictModelWrapper.Config) => config.setEnableLeafAssignment(enableLeafNodeAssignments)) ::
        ((config: EasyPredictModelWrapper.Config) => config.setEnableStagedProbabilities(enableStagedProbabilities)) ::
        Nil)
  }

  private def canGenerateContributions(model: GenModel): Boolean = {
    model match {
      case m: PredictContributionsFactory =>
        val modelCategory = model.getModelCategory
        if (modelCategory != ModelCategory.Regression && modelCategory != ModelCategory.Binomial) {
          logWarning(s"""
                        | Computing contributions on MOJO of type '${m.getModelCategory}' is only supported for regression
                        | and binomial model categories!
                        |""".stripMargin)
          false
        } else {
          true
        }
      case unsupported =>
        logWarning(s"Computing contributions is not allowed on MOJO of type '${unsupported.getClass}'!")
        false
    }
  }

  private def canGenerateLeafNodeAssignments(model: GenModel): Boolean = {
    model match {
      case _: TreeBackedMojoModel => true
      case _ =>
        logWarning("Computing leaf node assignments is only available on tree based models!")
        false
    }
  }

  private def canGenerateStageResults(model: GenModel): Boolean = {
    model match {
      case _: SharedTreeMojoModel => true
      case _ =>
        logWarning("Computing stage results is only available on tree based models except XGBoost!")
        false
    }
  }
}
