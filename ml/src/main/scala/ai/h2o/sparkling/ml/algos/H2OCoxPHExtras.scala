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
package ai.h2o.sparkling.ml.algos

import org.apache.spark.expose.Logging

import hex.coxph.CoxPHModel.CoxPHParameters

import ai.h2o.sparkling.ml.params.{H2OAutoMLInputParams, NullableStringParam, ParameterConstructorMethods}

private[algos] trait H2OCoxPHExtras extends H2OAlgorithm[CoxPHParameters]
  with ParameterConstructorMethods
  with Logging {

  protected final val foldCol = new NullableStringParam(
    this,
    "foldCol",
    "A name of a column determining folds when ``KFold`` holdoutStrategy is applied.")

  def getFoldCol(): String = $(foldCol)


  def setFoldCol(value: String): this.type = {
    set(foldCol, value)
  }

  setDefault(
    foldCol -> null
  )

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getFoldCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
