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

trait HasIgnoredCols extends H2OAlgoParamsBase {
  private val ignoredCols =
    new NullableStringArrayParam(this, "ignoredCols", "Names of columns to ignore for training.")

  setDefault(ignoredCols -> null)

  def getIgnoredCols(): Array[String] = $(ignoredCols)

  def setIgnoredCols(value: Array[String]): this.type = set(ignoredCols, value)

  private[sparkling] def getIgnoredColsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("ignored_columns" -> getIgnoredCols())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() + ("ignoredCols" -> "ignored_columns")
  }
}
