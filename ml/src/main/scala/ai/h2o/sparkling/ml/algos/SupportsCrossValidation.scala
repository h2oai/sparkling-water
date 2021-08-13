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

import org.apache.spark.ml.param.{BooleanParam, Params}

trait SupportsCrossValidation extends Params with H2OAlgoCommonUtils {

  def getFoldCol(): String

  def setFoldCol(value: String): this.type

  protected final val generateCrossValidationModels = new BooleanParam(
    this,
    "generateCrossValidationModels",
    "A flag indicating whether cross-validation models will be generated and appended to " +
      "the result model if cross-validation is applied")

  setDefault(generateCrossValidationModels -> false)

  def getGenerateCrossValidationModels(): Boolean = $(generateCrossValidationModels)

  def setGenerateCrossValidationModels(value: Boolean): this.type = set(generateCrossValidationModels, value)

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getFoldCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
