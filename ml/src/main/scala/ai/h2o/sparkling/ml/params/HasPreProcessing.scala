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

import ai.h2o.automl.preprocessing.PreprocessingStepDefinition
import ai.h2o.sparkling.H2OFrame
import org.apache.spark.expose.Logging

trait HasPreProcessing extends H2OAlgoParamsBase {
  private val preProcessing = nullableStringArrayParam(
    "preProcessing",
    "The list of pre-processing steps to run. Only 'TargetEncoding' is currently supported.")

  setDefault(preProcessing -> null)

  def getPreProcessing(): Array[String] = $(preProcessing)

  def setPreProcessing(value: Array[String]): this.type = {
    type EnumType = PreprocessingStepDefinition.Type
    val validated = EnumParamValidator.getValidatedEnumValues[EnumType](value, nullEnabled = true)
    set(preProcessing, validated)
  }

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    super.getH2OAlgorithmParams(trainingFrame) ++ getPreProcessingParams()
  }

  private[sparkling] def getPreProcessingParams(): Map[String, Any] = {
    val value = getPreProcessing()
    val valueToBackend = if (value == null) {
      null
    } else {
      value.map { enumValue =>
        val stepType = PreprocessingStepDefinition.Type.valueOf(enumValue)
        Map("type" -> stepType)
      }
    }
    Map("preprocessing" -> valueToBackend)
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("preProcessing" -> "preprocessing")
  }
}
