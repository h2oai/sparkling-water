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
import org.apache.spark.ml.param.{BooleanParam, Param, StringArrayParam}

trait H2OAlgorithmMOJOParams extends H2OBaseMOJOParams with Logging {

  //
  // Param definitions
  //
  protected final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "Prediction column name")

  protected final val detailedPredictionCol = new Param[String](
    this,
    "detailedPredictionCol",
    "Column containing additional prediction details, its content depends on the model type.")

  protected final val withContributions = new BooleanParam(
    this,
    "withContributions",
    "Enables or disables generating a sub-column of detailedPredictionCol containing Shapley values of original features.")

  protected final val featuresCols: StringArrayParam =
    new StringArrayParam(this, "featuresCols", "Name of feature columns")

  protected final val namedMojoOutputColumns: Param[Boolean] = new BooleanParam(
    this,
    "namedMojoOutputColumns",
    "Mojo Output is not stored in the array but in the properly named columns")

  protected final val withLeafNodeAssignments =
    new BooleanParam(this, "withLeafNodeAssignments", "Enables or disables computation of leaf node assignments.")

  protected final val withStageResults =
    new BooleanParam(this, "withStageResults", "Enables or disables computation of stage results.")

  //
  // Default values
  //
  setDefault(
    predictionCol -> H2OMOJOSettings.default.predictionCol,
    detailedPredictionCol -> H2OMOJOSettings.default.detailedPredictionCol,
    withContributions -> H2OMOJOSettings.default.withContributions,
    featuresCols -> Array.empty[String],
    namedMojoOutputColumns -> H2OMOJOSettings.default.namedMojoOutputColumns,
    withLeafNodeAssignments -> H2OMOJOSettings.default.withLeafNodeAssignments,
    withStageResults -> H2OMOJOSettings.default.withStageResults)

  //
  // Getters
  //
  def getPredictionCol(): String = $(predictionCol)

  def getDetailedPredictionCol(): String = $(detailedPredictionCol)

  def getWithContributions(): Boolean = $(withContributions)

  def getFeaturesCols(): Array[String] = $(featuresCols)

  @DeprecatedMethod(version = "3.40")
  def getNamedMojoOutputColumns(): Boolean = $(namedMojoOutputColumns)

  def getWithLeafNodeAssignments(): Boolean = $(withLeafNodeAssignments)

  def getWithStageResults(): Boolean = $(withStageResults)
}
