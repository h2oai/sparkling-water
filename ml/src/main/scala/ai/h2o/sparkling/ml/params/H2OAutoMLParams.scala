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

import ai.h2o.automl.Algo
import ai.h2o.sparkling.ml.algos.H2OAutoMLSortMetric
import hex.ScoreKeeper
import org.apache.spark.ml.param._

trait H2OAutoMLParams extends H2OCommonSupervisedParams with HasMonotoneConstraints {

  //
  // Param definitions
  //
  private val ignoredCols = new StringArrayParam(this, "ignoredCols", "Ignored column names")
  private val includeAlgos = new StringArrayParam(this, "includeAlgos", "Algorithms to include when using automl")
  private val excludeAlgos = new StringArrayParam(this, "excludeAlgos", "Algorithms to exclude when using automl")
  private val projectName = new NullableStringParam(
    this,
    "projectName",
    "Identifier for models that should be grouped together in the leaderboard" +
      " (e.g., airlines and iris)")
  private val maxRuntimeSecs =
    new DoubleParam(this, "maxRuntimeSecs", "Maximum time in seconds for automl to be running")
  private val stoppingRounds = new IntParam(this, "stoppingRounds", "Stopping rounds")
  private val stoppingTolerance = new DoubleParam(this, "stoppingTolerance", "Stopping tolerance")
  private val stoppingMetric = new Param[String](this, "stoppingMetric", "Stopping metric")
  private val sortMetric = new Param[String](this, "sortMetric", "Sort metric for the AutoML leaderboard")
  private val balanceClasses = new BooleanParam(this, "balanceClasses", "Ballance classes")
  private val classSamplingFactors = new NullableFloatArrayParam(this, "classSamplingFactors", "Class sampling factors")
  private val maxAfterBalanceSize = new FloatParam(this, "maxAfterBalanceSize", "Max after balance size")
  private val keepCrossValidationPredictions =
    new BooleanParam(this, "keepCrossValidationPredictions", "Keep cross Validation predictions")
  private val keepCrossValidationModels =
    new BooleanParam(this, "keepCrossValidationModels", "Keep cross validation models")
  private val maxModels = new IntParam(this, "maxModels", "Maximal number of models to be trained in AutoML")

  //
  // Default values
  //
  setDefault(
    ignoredCols -> Array.empty[String],
    includeAlgos -> Algo.values().map(_.name()),
    excludeAlgos -> Array.empty[String],
    projectName -> null, // will be automatically generated
    maxRuntimeSecs -> 0.0,
    stoppingRounds -> 3,
    stoppingTolerance -> 0.001,
    stoppingMetric -> ScoreKeeper.StoppingMetric.AUTO.name(),
    sortMetric -> H2OAutoMLSortMetric.AUTO.name(),
    balanceClasses -> false,
    classSamplingFactors -> null,
    maxAfterBalanceSize -> 5.0f,
    keepCrossValidationPredictions -> false,
    keepCrossValidationModels -> false,
    maxModels -> 0)

  //
  // Getters
  //
  def getIgnoredCols(): Array[String] = $(ignoredCols)

  def getIncludeAlgos(): Array[String] = $(includeAlgos)

  def getExcludeAlgos(): Array[String] = $(excludeAlgos)

  def getProjectName(): String = $(projectName)

  def getMaxRuntimeSecs(): Double = $(maxRuntimeSecs)

  def getStoppingRounds(): Int = $(stoppingRounds)

  def getStoppingTolerance(): Double = $(stoppingTolerance)

  def getStoppingMetric(): String = $(stoppingMetric)

  def getSortMetric(): String = $(sortMetric)

  def getBalanceClasses(): Boolean = $(balanceClasses)

  def getClassSamplingFactors(): Array[Float] = $(classSamplingFactors)

  def getMaxAfterBalanceSize(): Float = $(maxAfterBalanceSize)

  def getKeepCrossValidationPredictions(): Boolean = $(keepCrossValidationPredictions)

  def getKeepCrossValidationModels(): Boolean = $(keepCrossValidationModels)

  def getMaxModels(): Int = $(maxModels)

  //
  // Setters
  //
  def setIgnoredCols(value: Array[String]): this.type = set(ignoredCols, value)

  def setIncludeAlgos(value: Array[String]): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValues[Algo](value, nullEnabled = true)
    set(includeAlgos, validated)
  }

  def setExcludeAlgos(value: Array[String]): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValues[Algo](value, nullEnabled = true)
    set(excludeAlgos, validated)
  }

  def setProjectName(value: String): this.type = set(projectName, value)

  def setMaxRuntimeSecs(value: Double): this.type = set(maxRuntimeSecs, value)

  def setStoppingRounds(value: Int): this.type = set(stoppingRounds, value)

  def setStoppingTolerance(value: Double): this.type = set(stoppingTolerance, value)

  def setStoppingMetric(value: String): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[ScoreKeeper.StoppingMetric](value)
    set(stoppingMetric, validated)
  }

  def setSortMetric(value: String): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[H2OAutoMLSortMetric](value)
    set(sortMetric, validated)
  }

  def setBalanceClasses(value: Boolean): this.type = set(balanceClasses, value)

  def setClassSamplingFactors(value: Array[Float]): this.type = set(classSamplingFactors, value)

  def setMaxAfterBalanceSize(value: Float): this.type = set(maxAfterBalanceSize, value)

  def setKeepCrossValidationPredictions(value: Boolean): this.type = set(keepCrossValidationPredictions, value)

  def setKeepCrossValidationModels(value: Boolean): this.type = set(keepCrossValidationModels, value)

  def setMaxModels(value: Int): this.type = set(maxModels, value)
}
