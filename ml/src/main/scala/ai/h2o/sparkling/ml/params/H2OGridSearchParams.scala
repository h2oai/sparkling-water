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

import java.util

import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.ml.algos.{H2OGridSearch, H2OSupervisedAlgorithm}
import ai.h2o.sparkling.ml.internals.H2OMetric
import hex.grid.HyperSpaceSearchCriteria
import hex.{Model, ScoreKeeper}
import org.apache.spark.expose.Logging
import org.apache.spark.ml.param._

import scala.collection.JavaConverters._
import scala.collection.mutable

trait H2OGridSearchParams extends Params with Logging {

  //
  // Param definitions
  //
  private val algo = new AlgoParam(this, "algo", "Specifies the algorithm for grid search")
  private val hyperParameters = new HyperParamsParam(this, "hyperParameters", "Hyper Parameters")
  private val strategy = new Param[String](this, "strategy", "Search criteria strategy")
  private val maxRuntimeSecs = new DoubleParam(this, "maxRuntimeSecs", "maxRuntimeSecs")
  private val maxModels = new IntParam(this, "maxModels", "maxModels")
  private val stoppingRounds =
    new IntParam(this, "stoppingRounds", "Early stopping based on convergence of stoppingMetric")
  private val stoppingTolerance = new DoubleParam(
    this,
    "stoppingTolerance",
    "Relative tolerance for metric-based" +
      " stopping criterion: stop if relative improvement is not at least this much.")
  private val stoppingMetric = new Param[String](this, "stoppingMetric", "Stopping Metric")
  private val selectBestModelBy = new Param[String](
    this,
    "selectBestModelBy",
    "Select best model by specific metric." +
      "If this value is not specified that the first model os taken.")
  private val parallelism = new IntParam(
    this,
    "parallelism",
    """Level of model-building parallelism, the possible values are:
      | 0 -> H2O selects parallelism level based on cluster configuration, such as number of cores
      | 1 -> Sequential model building, no parallelism
      | n>1 -> n models will be built in parallel if possible""".stripMargin)
  private val seed = new LongParam(this, "seed", "Used to specify seed to reproduce the model run")

  //
  // Default values
  //
  setDefault(
    algo -> null,
    hyperParameters -> Map.empty[String, Array[AnyRef]].asJava,
    strategy -> HyperSpaceSearchCriteria.Strategy.Cartesian.name(),
    maxRuntimeSecs -> 0,
    maxModels -> 0,
    stoppingRounds -> 0,
    stoppingTolerance -> 0.001,
    stoppingMetric -> ScoreKeeper.StoppingMetric.AUTO.name(),
    selectBestModelBy -> H2OMetric.AUTO.name(),
    parallelism -> 1,
    seed -> -1)

  val propagateToAlgorithm: mutable.HashMap[String, Any] = new mutable.HashMap[String, Any]()

  //
  // Getters
  //
  def getAlgo(): H2OSupervisedAlgorithm[_ <: Model.Parameters] = $(algo)

  def getHyperParameters(): util.Map[String, Array[AnyRef]] = $(hyperParameters)

  def getStrategy(): String = $(strategy)

  def getMaxRuntimeSecs(): Double = $(maxRuntimeSecs)

  def getMaxModels(): Int = $(maxModels)

  def getStoppingRounds(): Int = $(stoppingRounds)

  def getStoppingTolerance(): Double = $(stoppingTolerance)

  def getStoppingMetric(): String = $(stoppingMetric)

  def getSelectBestModelBy(): String = $(selectBestModelBy)

  def getParallelism(): Int = $(parallelism)

  def getSeed(): Long = $(seed)

  @DeprecatedMethod("the 'getPredictionCol' method of a given algorithm", "3.32")
  def getPredictionCol(): String = getAlgo().getPredictionCol()

  @DeprecatedMethod("the 'getDetailedPredictionCol' method of a given algorithm", "3.32")
  def getDetailedPredictionCol(): String = getAlgo().getDetailedPredictionCol()

  @DeprecatedMethod("the 'getWithDetailedPredictionCol' method of a given algorithm", "3.32")
  def getWithDetailedPredictionCol(): Boolean = getAlgo().getWithDetailedPredictionCol()

  @DeprecatedMethod("the 'getFeaturesCols' method of a given algorithm", "3.32")
  def getFeaturesCols(): Array[String] = getAlgo().getFeaturesCols()

  @DeprecatedMethod("the 'getConvertUnknownCategoricalLevelsToNa' method of a given algorithm", "3.32")
  def getConvertUnknownCategoricalLevelsToNa(): Boolean = getAlgo().getConvertUnknownCategoricalLevelsToNa()

  @DeprecatedMethod("the 'getConvertInvalidNumbersToNa' method of a given algorithm", "3.32")
  def getConvertInvalidNumbersToNa(): Boolean = getAlgo().getConvertInvalidNumbersToNa()

  @DeprecatedMethod("the 'getNamedMojoOutputColumns' method of a given algorithm", "3.32")
  def getNamedMojoOutputColumns(): Boolean = getAlgo().getNamedMojoOutputColumns()

  @DeprecatedMethod("the 'getFoldCol' method of a given algorithm", "3.32")
  def getFoldCol(): String = getAlgo().getFoldCol()

  @DeprecatedMethod("the 'getWeightCol' method of a given algorithm", "3.32")
  def getWeightCol(): String = getAlgo().getWeightCol()

  @DeprecatedMethod("the 'getSplitRatio' method of a given algorithm", "3.32")
  def getSplitRatio(): Double = getAlgo().getSplitRatio()

  @DeprecatedMethod("the 'getNfolds' method of a given algorithm", "3.32")
  def getNfolds(): Int = getAlgo().getNfolds()

  @DeprecatedMethod(version = "3.32")
  def getAllStringColumnsToCategorical(): Boolean = false

  @DeprecatedMethod("the 'getColumnsToCategorical' method of a given algorithm", "3.32")
  def getColumnsToCategorical(): Array[String] = getAlgo().getColumnsToCategorical()

  @DeprecatedMethod("the 'getOffsetCol' method of a given algorithm", "3.32")
  def getOffsetCol(): String = getAlgo().getOffsetCol()

  @DeprecatedMethod("the 'getLabelCol' method of a given algorithm", "3.32")
  def getLabelCol(): String = getAlgo().getLabelCol()

  //
  // Setters
  //
  def setAlgo(value: H2OSupervisedAlgorithm[_ <: Model.Parameters]): this.type = {
    H2OGridSearch.SupportedAlgos.checkIfSupported(value)
    for ((paramName, paramValue) <- propagateToAlgorithm) {
      value.set(value.getParam(paramName), paramValue)
    }
    set(algo, value)
  }

  def setHyperParameters(value: Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.asJava)

  def setHyperParameters(value: mutable.Map[String, Array[AnyRef]]): this.type =
    set(hyperParameters, value.toMap.asJava)

  def setHyperParameters(value: java.util.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value)

  def setStrategy(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[HyperSpaceSearchCriteria.Strategy](value)
    set(strategy, validated)
  }

  def setMaxRuntimeSecs(value: Double): this.type = set(maxRuntimeSecs, value)

  def setMaxModels(value: Int): this.type = set(maxModels, value)

  def setStoppingRounds(value: Int): this.type = set(stoppingRounds, value)

  def setStoppingTolerance(value: Double): this.type = set(stoppingTolerance, value)

  def setStoppingMetric(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[ScoreKeeper.StoppingMetric](value)
    set(stoppingMetric, validated)
  }

  def setSelectBestModelBy(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[H2OMetric](value)
    set(selectBestModelBy, validated)
  }

  def setParallelism(value: Int): this.type = set(parallelism, value)

  def setSeed(value: Long): this.type = set(seed, value)

  @DeprecatedMethod("the 'setFoldCol' method of a given algorithm", "3.32")
  def setFoldCol(columnName: String): this.type = {
    propagateToAlgorithm.put("foldCol", columnName)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setFoldCol(columnName)
    this
  }

  @DeprecatedMethod("the 'setWeightCol' method of a given algorithm", "3.32")
  def setWeightCol(columnName: String): this.type = {
    propagateToAlgorithm.put("weightCol", columnName)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setWeightCol(columnName)
    this
  }

  @DeprecatedMethod("the 'setSplitRatio' method of a given algorithm", "3.32")
  def setSplitRatio(ratio: Double): this.type = {
    propagateToAlgorithm.put("splitRatio", ratio)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setSplitRatio(ratio)
    this
  }

  @DeprecatedMethod("the 'setNfolds' method of a given algorithm", "3.32")
  def setNfolds(value: Int): this.type = {
    propagateToAlgorithm.put("nfolds", value)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setNfolds(value)
    this
  }

  @DeprecatedMethod(version = "3.32")
  def setAllStringColumnsToCategorical(value: Boolean): this.type = this

  @DeprecatedMethod("the 'setColumnsToCategorical' method of a given algorithm", "3.32")
  def setColumnsToCategorical(first: String, others: String*): this.type = {
    propagateToAlgorithm.put("columnsToCategorical", Array(first) ++ others)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setColumnsToCategorical(first, others: _*)
    this
  }

  @DeprecatedMethod("the 'setColumnsToCategorical' method of a given algorithm", "3.32")
  def setColumnsToCategorical(columns: Array[String]): this.type = {
    propagateToAlgorithm.put("columnsToCategorical", columns)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setColumnsToCategorical(columns)
    this
  }

  @DeprecatedMethod("the 'setPredictionCol' method of a given algorithm", "3.32")
  def setPredictionCol(columnName: String): this.type = {
    propagateToAlgorithm.put("predictionCol", columnName)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setPredictionCol(columnName)
    this
  }

  @DeprecatedMethod("the 'setDetailedPredictionCol' method of a given algorithm", "3.32")
  def setDetailedPredictionCol(columnName: String): this.type = {
    propagateToAlgorithm.put("detailedPredictionCol", columnName)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setDetailedPredictionCol(columnName)
    this
  }

  @DeprecatedMethod("the 'setWithDetailedPredictionCol' method of a given algorithm", "3.32")
  def setWithDetailedPredictionCol(enabled: Boolean): this.type = {
    propagateToAlgorithm.put("withDetailedPredictionCol", enabled)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setWithDetailedPredictionCol(enabled)
    this
  }

  @DeprecatedMethod("the 'setFeaturesCol' method of a given algorithm", "3.32")
  def setFeaturesCol(first: String): this.type = {
    propagateToAlgorithm.put("featuresCols", Array(first))
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setFeaturesCol(first)
    this
  }

  @DeprecatedMethod("the 'setFeaturesCols' method of a given algorithm", "3.32")
  def setFeaturesCols(first: String, others: String*): this.type = {
    propagateToAlgorithm.put("featuresCols", Array(first) ++ others)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setFeaturesCols(first, others: _*)
    this
  }

  @DeprecatedMethod("the 'setFeaturesCols' method of a given algorithm", "3.32")
  def setFeaturesCols(columnNames: Array[String]): this.type = {
    propagateToAlgorithm.put("featuresCols", columnNames)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setFeaturesCols(columnNames)
    this
  }

  @DeprecatedMethod("the 'setConvertUnknownCategoricalLevelsToNa' method of a given algorithm", "3.32")
  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type = {
    propagateToAlgorithm.put("convertUnknownCategoricalLevelsToNa", value)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setConvertUnknownCategoricalLevelsToNa(value)
    this
  }

  @DeprecatedMethod("the 'setConvertInvalidNumbersToNa' method of a given algorithm", "3.32")
  def setConvertInvalidNumbersToNa(value: Boolean): this.type = {
    propagateToAlgorithm.put("convertInvalidNumbersToNa", value)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setConvertInvalidNumbersToNa(value)
    this
  }

  @DeprecatedMethod("the 'setNamedMojoOutputColumns' method of a given algorithm", "3.32")
  def setNamedMojoOutputColumns(value: Boolean): this.type = {
    propagateToAlgorithm.put("namedMojoOutputColumns", value)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setNamedMojoOutputColumns(value)
    this
  }

  @DeprecatedMethod("the 'setLabelCol' method of a given algorithm", "3.32")
  def setLabelCol(columnName: String): this.type = {
    propagateToAlgorithm.put("labelCol", columnName)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setLabelCol(columnName)
    this
  }

  @DeprecatedMethod("the 'setOffsetCol' method of a given algorithm", "3.32")
  def setOffsetCol(columnName: String): this.type = {
    propagateToAlgorithm.put("offsetCol", columnName)
    val algorithm = getAlgo()
    if (algorithm != null) algorithm.setOffsetCol(columnName)
    this
  }
}
