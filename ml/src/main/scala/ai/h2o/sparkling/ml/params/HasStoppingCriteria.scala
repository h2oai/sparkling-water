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

import ai.h2o.sparkling.ml.params.H2OAlgoParamsHelper.getValidatedEnumValue
import hex.Model.Parameters
import hex.ScoreKeeper.StoppingMetric
import org.apache.spark.ml.param.{DoubleParam => DblParam, IntParam, Param}

trait HasStoppingCriteria[P <: Parameters] extends H2OAlgoParamsHelper[P] {
  private val stoppingRounds = new IntParam(this, "stoppingRounds", "Stopping rounds")
  private val stoppingTolerance = new DblParam(this, "stoppingTolerance", "Stopping tolerance")
  private val stoppingMetric = new Param[String](this, "stoppingMetric", "Stopping metric")

  //
  // Default values
  //
  setDefault(
    stoppingRounds -> parameters._stopping_rounds,
    stoppingMetric -> parameters._stopping_metric.name(),
    stoppingTolerance -> parameters._stopping_tolerance
  )

  //
  // Getters
  //
  def getStoppingRounds() = $(stoppingRounds)

  def getStoppingMetric() = $(stoppingMetric)

  def getStoppingTolerance() = $(stoppingTolerance)

  //
  // Setters
  //
  def setStoppingRounds(value: Int): this.type = set(stoppingRounds, value)

  def setStoppingMetric(value: String) : this.type = {
    val validated = getValidatedEnumValue[StoppingMetric](value)
    set(stoppingMetric, validated)
  }

  def setStoppingTolerance(value: Double): this.type = set(stoppingTolerance, value)
}
