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
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import hex.schemas.DeepLearningV3.DeepLearningParametersV3

trait H2ODeepLearningParams extends H2OAlgoSupervisedParams[DeepLearningParameters]
  with HasStoppingCriteria[DeepLearningParameters]
  with HasQuantileAlpha {

  type H2O_SCHEMA = DeepLearningParametersV3

  protected def paramTag = reflect.classTag[DeepLearningParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  private val epochs = doubleParam("epochs")
  private val l1 = doubleParam("l1")
  private val l2 = doubleParam("l2")
  private val hidden = intArrayParam("hidden")
  private val reproducible = booleanParam("reproducible")
  private val activation = stringParam("activation")
  private val forceLoadBalance = booleanParam("forceLoadBalance")
  //
  // Default values
  //
  setDefault(
    epochs -> parameters._epochs,
    l1 -> parameters._l1,
    l2 -> parameters._l2,
    hidden -> parameters._hidden,
    reproducible -> parameters._reproducible,
    activation -> parameters._activation.name(),
    forceLoadBalance -> parameters._force_load_balance)

  //
  // Getters
  //
  def getEpochs(): Double = $(epochs)

  def getL1(): Double = $(l1)

  def getL2(): Double = $(l2)

  def getHidden(): Array[Int] = $(hidden)

  def getReproducible(): Boolean = $(reproducible)

  def getActivation(): String = $(activation)

  def getForceLoadBalance(): Boolean = $(forceLoadBalance)

  //
  // Setters
  //
  def setEpochs(value: Double): this.type = set(epochs, value)

  def setL1(value: Double): this.type = set(l1, value)

  def setL2(value: Double): this.type = set(l2, value)

  def setHidden(value: Array[Int]): this.type = set(hidden, value)

  def setReproducible(value: Boolean): this.type = set(reproducible, value)

  def setActivation(value: String): this.type = {
    set(activation, getValidatedEnumValue[Activation](value))
  }

  def setForceLoadBalance(value: Boolean): this.type = set(forceLoadBalance, value)

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++
      Map(
        "epochs" -> getEpochs(),
        "l1" -> getL1(),
        "l2" -> getL2(),
        "hidden" -> getHidden(),
        "reproducible" -> getReproducible(),
        "stopping_rounds" -> getStoppingRounds(),
        "stopping_metric" -> getStoppingMetric(),
        "stopping_tolerance" -> getStoppingTolerance(),
        "quantile_alpha" -> getQuantileAlpha(),
        "activation" -> getActivation()
      )
  }
}
