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

package ai.h2o.sparkling.api.generation.common

object ParameterNameConverter {
  val h2oToSWExceptions: Map[String, String] = Map(
    "response_column" -> "labelCol",
    "validation_response_column" -> "validationLabelCol",
    "weights_column" -> "weightCol",
    "lambda" -> "lambdaValue",
    "alpha" -> "alphaValue",
    "colsample_bylevel" -> "colSampleByLevel",
    "colsample_bytree" -> "colSampleByTree",
    "colsample_bynode" -> "colSampleByNode",
    "rand_family" -> "randomFamily",
    "rand_link" -> "randomLink",
    "calibration_frame" -> "calibrationDataFrame")

  val conversionRules: Map[String, String] = Map("Column" -> "Col")

  def convertFromH2OToSW(parameterName: String): String = {
    val parts = parameterName.split("_")
    val capitalizedParts = parts.head +: parts.tail.map(_.capitalize)
    val regularValue = conversionRules.foldLeft(capitalizedParts.mkString) {
      case (input, (from, to)) => input.replace(from, to)
    }
    h2oToSWExceptions.getOrElse(parameterName, regularValue)
  }
}
