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
import ai.h2o.sparkling.ml.algos.H2OAlgoCommonUtils

trait HasGamCols extends H2OAlgoParamsBase with H2OAlgoCommonUtils {
  protected val gamCols = nullableStringArrayArrayParam(
    name = "gamCols",
    doc = "Arrays of predictor column names for gam for smoothers using single or " +
      "multiple predictors like {{'c1'},{'c2','c3'},{'c4'},...}")

  setDefault(gamCols -> null)

  def getGamCols(): Array[Array[String]] = $(gamCols)

  def setGamCols(value: Array[Array[String]]): this.type = set(gamCols, value)

  def setGamCols(value: Array[String]): this.type = setGamCols(value.map(Array(_)))

  private[sparkling] def getGamColsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("gam_columns" -> getGamCols())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() + ("gamCols" -> "gam_columns")
  }

  override private[sparkling] def getAdditionalCols(): Seq[String] = {
    val gamCols = getGamCols()
    if (gamCols != null) {
      super.getAdditionalCols() ++ gamCols.flatten.distinct
    } else {
      super.getAdditionalCols()
    }
  }
}
