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

import ai.h2o.sparkling.ml.models.H2OMOJOModel.{extractFeatureImportances, extractModelCategory, extractParams, extractScoringHistory, getModelDetails}
import ai.h2o.sparkling.ml.params.{MapStringDoubleParam, MapStringStringParam, NullableDataFrameParam, NullableStringParam}
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.DataFrame

trait H2OMOJOCommonParameters extends Params{
  protected final val modelDetails: NullableStringParam =
    new NullableStringParam(this, "modelDetails", "Raw details of this model.")
  protected final val trainingMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "trainingMetrics", "Training metrics.")
  protected final val validationMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "validationMetrics", "Validation metrics.")
  protected final val crossValidationMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "crossValidationMetrics", "Cross Validation metrics.")
  protected final val trainingParams: MapStringStringParam =
    new MapStringStringParam(this, "trainingParams", "Training params")
  protected final val modelCategory: NullableStringParam =
    new NullableStringParam(this, "modelCategory", "H2O's model category")
  protected final val scoringHistory: NullableDataFrameParam =
    new NullableDataFrameParam(this, "scoringHistory", "Scoring history acquired during the model training.")
  protected final val featureImportances: NullableDataFrameParam =
    new NullableDataFrameParam(this, "featureImportances", "Feature imporanteces.")

  setDefault(
    modelDetails -> null,
    trainingMetrics -> Map.empty[String, Double],
    validationMetrics -> Map.empty[String, Double],
    crossValidationMetrics -> Map.empty[String, Double],
    trainingParams -> Map.empty[String, String],
    modelCategory -> null,
    scoringHistory -> null,
    featureImportances -> null)

  def getTrainingMetrics(): Map[String, Double] = $(trainingMetrics)

  def getValidationMetrics(): Map[String, Double] = $(validationMetrics)

  def getCrossValidationMetrics(): Map[String, Double] = $(crossValidationMetrics)

  def getCurrentMetrics(): Map[String, Double] = {
    val nfolds = $(trainingParams).get("nfolds")
    val validationFrame = $(trainingParams).get("validation_frame")
    if (nfolds.isDefined && nfolds.get.toInt > 1) {
      getCrossValidationMetrics()
    } else if (validationFrame.isDefined) {
      getValidationMetrics()
    } else {
      getTrainingMetrics()
    }
  }

  def getTrainingParams(): Map[String, String] = $(trainingParams)

  def getModelCategory(): String = $(modelCategory)

  def getModelDetails(): String = $(modelDetails)

  def getScoringHistory(): DataFrame = $(scoringHistory)

  def getFeatureImportances(): DataFrame = $(featureImportances)

  def setCommonParameters(): Unit = {
    model.set(model.modelDetails -> getModelDetails(modelJson))
    model.set(model.trainingMetrics -> trainingMetrics)
    model.set(model.validationMetrics -> validationMetrics)
    model.set(model.crossValidationMetrics -> crossValidationMetrics)
    model.set(model.trainingParams -> extractParams(modelJson))
    model.set(model.modelCategory -> extractModelCategory(modelJson).toString)
    model.set(model.scoringHistory -> extractScoringHistory(modelJson))
    model.set(model.featureImportances -> extractFeatureImportances(modelJson))
  }
}
