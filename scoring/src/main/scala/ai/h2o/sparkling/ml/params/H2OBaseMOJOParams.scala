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
package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.ml.models.H2OMOJOSettings
import org.apache.spark.expose.Logging
import org.apache.spark.ml.param._

/**
  * Parameters available on the algorithm & MOJO Model
  */
trait H2OBaseMOJOParams extends Params with Logging {

  //
  // Param definitions
  //
  protected final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "Prediction column name")

  protected final val detailedPredictionCol = new Param[String](
    this,
    "detailedPredictionCol",
    "Column containing additional prediction details, its content depends on the model type.")

  protected final val withDetailedPredictionCol = new BooleanParam(
    this,
    "withDetailedPredictionCol",
    "Enables or disables generating additional prediction column, but with more details")

  protected final val withContributions = new BooleanParam(
    this,
    "withContributions",
    "Enables or disables generating a sub-column of detailedPredictionCol containing Shapley values.")

  protected final val featuresCols: StringArrayParam =
    new StringArrayParam(this, "featuresCols", "Name of feature columns")

  protected final val convertUnknownCategoricalLevelsToNa = new BooleanParam(
    this,
    "convertUnknownCategoricalLevelsToNa",
    "If set to 'true', the model converts unknown categorical levels to NA during making predictions.")

  protected final val convertInvalidNumbersToNa = new BooleanParam(
    this,
    "convertInvalidNumbersToNa",
    "If set to 'true', the model converts invalid numbers to NA during making predictions.")

  protected final val namedMojoOutputColumns: Param[Boolean] = new BooleanParam(
    this,
    "namedMojoOutputColumns",
    "Mojo Output is not stored in the array but in the properly named columns")

  protected final val withLeafNodeAssignments =
    new BooleanParam(this, "withLeafNodeAssignments", "Enables or disables computation of leaf node assignments.")

  protected final val withStageResults =
    new BooleanParam(this, "withStageResults", "Enables or disables computation of stage results.")

  protected final val withReconstructedData = new BooleanParam(
    this,
    "withReconstructedData",
    "Enables or disables computation of reconstructed data for dimension reduction problems.")

  //
  //
  // Default values
  //
  setDefault(
    predictionCol -> H2OMOJOSettings.default.predictionCol,
    detailedPredictionCol -> H2OMOJOSettings.default.detailedPredictionCol,
    withContributions -> H2OMOJOSettings.default.withContributions,
    featuresCols -> Array.empty[String],
    convertUnknownCategoricalLevelsToNa -> H2OMOJOSettings.default.convertUnknownCategoricalLevelsToNa,
    convertInvalidNumbersToNa -> H2OMOJOSettings.default.convertInvalidNumbersToNa,
    namedMojoOutputColumns -> H2OMOJOSettings.default.namedMojoOutputColumns,
    withLeafNodeAssignments -> H2OMOJOSettings.default.withLeafNodeAssignments,
    withStageResults -> H2OMOJOSettings.default.withStageResults,
    withReconstructedData -> H2OMOJOSettings.default.withReconstructedData)

  //
  // Getters
  //
  def getPredictionCol(): String = $(predictionCol)

  def getDetailedPredictionCol(): String = $(detailedPredictionCol)

  @DeprecatedMethod(version = "3.34")
  def getWithDetailedPredictionCol(): Boolean = true

  def getWithContributions(): Boolean = $(withContributions)

  def getFeaturesCols(): Array[String] = $(featuresCols)

  def getConvertUnknownCategoricalLevelsToNa(): Boolean = $(convertUnknownCategoricalLevelsToNa)

  def getConvertInvalidNumbersToNa(): Boolean = $(convertInvalidNumbersToNa)

  def getNamedMojoOutputColumns(): Boolean = $(namedMojoOutputColumns)

  def getWithLeafNodeAssignments(): Boolean = $(withLeafNodeAssignments)

  def getWithStageResults(): Boolean = $(withStageResults)

  def getWithReconstructedData(): Boolean = $(withReconstructedData)
}
