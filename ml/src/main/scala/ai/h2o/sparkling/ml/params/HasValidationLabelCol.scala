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

trait HasValidationLabelCol extends H2OAlgoParamsBase with H2OAlgoCommonUtils {
  protected val validationLabelCol = stringParam(
    name = "validationLabelCol",
    doc = "(experimental) Name of the label column in the validation data frame. " +
      "The label column should be a string column with two distinct values indicating the anomaly. " +
      "The negative value must be alphabetically smaller than the positive value. (E.g. '0'/'1', 'False'/'True'")

  setDefault(validationLabelCol -> "label")

  def getValidationLabelCol(): String = $(validationLabelCol)

  def setValidationLabelCol(value: String): this.type = set(validationLabelCol, value)

  private[sparkling] def getValidationLabelColParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("validation_response_column" -> getValidationLabelCol())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() + ("validationLabelCol" -> "validation_response_column")
  }

  override private[sparkling] def getAdditionalValidationCols(): Seq[String] = {
    super.getAdditionalValidationCols() :+ getValidationLabelCol()
  }
}
