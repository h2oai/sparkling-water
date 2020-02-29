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

import hex.schemas.DRFV3.DRFParametersV3
import hex.tree.drf.DRFModel.DRFParameters

trait H2ODRFParams extends H2OAlgoSharedTreeParams[DRFParameters] {

  type H2O_SCHEMA = DRFParametersV3

  protected def paramTag = reflect.classTag[DRFParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  private val binomialDoubleTrees = booleanParam("binomialDoubleTrees")
  private val mtries = intParam("mtries")

  //
  // Default values
  //
  setDefault(
    binomialDoubleTrees -> false,
    mtries -> -1,
    maxDepth -> 20, // DRF overrides this default value from SharedTreeParams
    minRows -> 1 // DRF overrides this default value from SharedTreeParams
  )

  //
  // Getters
  //
  def getBinomialDoubleTrees(): Boolean = $(binomialDoubleTrees)

  def getMtries(): Double = $(mtries)


  //
  // Setters
  //
  def setBinomialDoubleTrees(value: Boolean): this.type = set(binomialDoubleTrees, value)

  def setMtries(value: Int): this.type = set(mtries, value)

  override protected def updateH2OParamsREST: Map[String, Any] = {
    super.updateH2OParamsREST() ++
    Map(
      "binomial_double_trees" -> getBinomialDoubleTrees(),
      "mtries" -> getMtries()
    )
  }

  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._binomial_double_trees = $(binomialDoubleTrees)
    parameters._mtries = $(mtries)
  }
}
