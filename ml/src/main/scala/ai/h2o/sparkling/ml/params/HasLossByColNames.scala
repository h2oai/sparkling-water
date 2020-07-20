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

trait HasLossByColNames extends H2OAlgoParamsBase {
  private val lossByColNames = new NullableStringArrayParam(
    this,
    "lossByColNames",
    "Columns names for which loss function will be overridden by the 'lossByCol' parameter")

  setDefault(lossByColNames -> null)

  def getLossByColNames(): Array[String] = $(lossByColNames)

  def setLossByColNames(value: Array[String]): this.type = set(lossByColNames, value)

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    val names = getLossByColNames()
    val indices = if (names == null) {
      null
    } else {
      val frameColumns = trainingFrame.columnNames
      val indices = names.map(frameColumns.indexOf)
      indices
    }

    super.getH2OAlgorithmParams(trainingFrame) ++ Map("loss_by_col_idx" -> indices)
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("lossByColNames" -> "loss_by_col_idx")
  }

}
