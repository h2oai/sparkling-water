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

import ai.h2o.sparkling.ml.algos.{H2OAlgorithm, H2OGridSearch}
import ai.h2o.sparkling.ml.internals.H2OMetric
import hex.Model
import org.apache.spark.ml.param._

import scala.collection.JavaConverters._
import scala.collection.mutable

trait H2OGridSearchParams
  extends H2OGridSearchRandomDiscreteCriteriaParams
  with H2OGridSearchCartesianCriteriaParams
  with H2OGridSearchCommonCriteriaParams {

  //
  // Param definitions
  //
  private val algo = new AlgoParam(this, "algo", "Specifies the algorithm for grid search")
  private val hyperParameters = new HyperParamsParam(this, "hyperParameters", "Hyper Parameters")
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

  //
  // Default values
  //
  setDefault(
    algo -> null,
    hyperParameters -> Map.empty[String, Array[AnyRef]].asJava,
    selectBestModelBy -> H2OMetric.AUTO.name(),
    parallelism -> 1)

  //
  // Getters
  //
  def getAlgo(): H2OAlgorithm[_ <: Model.Parameters] = $(algo)

  def getHyperParameters(): util.Map[String, Array[AnyRef]] = $(hyperParameters)

  def getSelectBestModelBy(): String = $(selectBestModelBy)

  def getParallelism(): Int = $(parallelism)

  //
  // Setters
  //
  def setAlgo(value: H2OAlgorithm[_ <: Model.Parameters]): this.type = {
    H2OGridSearch.SupportedAlgos.checkIfSupported(value)
    set(algo, value)
  }

  def setHyperParameters(value: Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.asJava)

  def setHyperParameters(value: mutable.Map[String, Array[AnyRef]]): this.type =
    set(hyperParameters, value.toMap.asJava)

  def setHyperParameters(value: java.util.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value)

  def setSelectBestModelBy(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[H2OMetric](value)
    set(selectBestModelBy, validated)
  }

  def setParallelism(value: Int): this.type = set(parallelism, value)
}
