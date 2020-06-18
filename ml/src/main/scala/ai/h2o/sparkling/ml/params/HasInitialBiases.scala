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

import org.apache.spark.ml.linalg.DenseVector

trait HasInitialBiases extends H2OAlgoParamsBase {
  private val initialBiases = new NullableVectorArrayParam(
    this,
    "initialBiases",
    "A array of weight vectors to be used for bias initialization of every network layer." +
      "If this parameter is set, the parameter 'initialWeights' has to be set as well.")

  setDefault(initialBiases -> null)

  def getInitialBiases(): Array[DenseVector] = $(initialBiases)

  def setInitialBiases(value: Array[DenseVector]): this.type = set(initialBiases, value)

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++ Map("initial_biases" -> convertVectorArrayToH2OFrameKeyArray(getInitialBiases()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("initialBiases" -> "initial_biases")
  }
}
