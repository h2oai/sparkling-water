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

import ai.h2o.sparkling.ml.algos.{H2OGridSearch, H2OSupervisedAlgorithm}
import ai.h2o.sparkling.ml.internals.H2OMetric
import hex.grid.HyperSpaceSearchCriteria
import hex.{Model, ScoreKeeper}
import org.apache.spark.ml.param._

import scala.collection.JavaConverters._
import scala.collection.mutable

trait H2OGridSearchParams extends H2OCommonSupervisedParams {

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
  private val nfolds = new IntParam(this, "nfolds", "Number of fold columns")

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
    seed -> -1,
    nfolds -> 0)

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

  def getNfolds(): Int = $(nfolds)

  //
  // Setters
  //
  def setAlgo(value: H2OSupervisedAlgorithm[_ <: Model.Parameters]): this.type = {
    H2OGridSearch.SupportedAlgos.checkIfSupported(value)
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

  def setNfolds(value: Int): this.type = set(nfolds, value)
}
