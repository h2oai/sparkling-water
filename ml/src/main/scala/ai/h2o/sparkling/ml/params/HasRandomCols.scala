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

trait HasRandomCols extends H2OAlgoParamsBase with H2OAlgoCommonUtils {
  private val randomCols = new NullableStringArrayParam(
    this,
    "randomCols",
    "Names of random columns for HGLM.")

  setDefault(randomCols -> null)

  def getRandomCols(): Array[String] = $(randomCols)

  def setRandomCols(value: Array[String]): this.type = set(randomCols, value)

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    val randomColumnNames = getRandomCols()
    val indices = if (randomColumnNames == null) {
      null
    } else {
      val frameColumns = trainingFrame.columnNames
      val indices = randomColumnNames.map(frameColumns.indexOf)
      indices
    }

    val result = super.getH2OAlgorithmParams(trainingFrame) + ("random_columns" -> indices)


    if (randomColumnNames == null) {
      result
    } else {
      val ignoredColumns = result.getOrElse("ignored_columns", null).asInstanceOf[Array[String]]
      if (ignoredColumns == null) {
        result + ("ignored_columns" -> randomColumnNames)
      } else {
        result + ("ignored_columns" -> (ignoredColumns ++ randomColumnNames))
      }
    }
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("randomCols" -> "random_columns")
  }

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    val randomCols = getRandomCols()
    if (randomCols == null) {
      super.getExcludedCols()
    } else {
      super.getExcludedCols() ++ randomCols
    }
  }
}
