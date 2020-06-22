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

trait HasUserPoints extends H2OAlgoParamsBase {
  private val userPoints = new NullableDoubleArrayArrayParam(
    this,
    "userPoints",
    "This option allows" +
      " you to specify array of points, where each point represents coordinates of an initial cluster center. The user-specified" +
      " points must have the same number of columns as the training observations. The number of rows must equal" +
      " the number of clusters.")

  setDefault(userPoints -> null)

  def getUserPoints(): Array[Array[Double]] = $(userPoints)

  def setUserPoints(value: Array[Array[Double]]): this.type = set(userPoints, value)

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    super.getH2OAlgorithmParams(trainingFrame) ++ Map("user_points" -> convert2dArrayToH2OFrame(getUserPoints()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("userPoints" -> "user_points")
  }
}
