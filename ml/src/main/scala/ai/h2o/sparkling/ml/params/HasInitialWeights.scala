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

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.ml.linalg.DenseMatrix

trait HasInitialWeights extends H2OAlgoParamsBase {
  private val initialWeights = new NullableMatrixArrayParam(
    this,
    "initialWeights",
    "A array of weight matrices to be used for initialization of the neural network. " +
      "If this parameter is set, the parameter 'initialBiases' has to be set as well.")

  setDefault(initialWeights -> null)

  def getInitialWeights(): Array[DenseMatrix] = $(initialWeights)

  def setInitialWeights(value: Array[DenseMatrix]): this.type = set(initialWeights, value)

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    super.getH2OAlgorithmParams(trainingFrame) ++
      Map("initial_weights" -> convertMatrixToH2OFrameKeyArray(getInitialWeights()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("initialWeights" -> "initial_weights")
  }
}
