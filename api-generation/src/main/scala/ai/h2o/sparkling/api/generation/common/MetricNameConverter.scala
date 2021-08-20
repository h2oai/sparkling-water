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

object MetricNameConverter {
  val h2oToSWExceptions: Map[String, (String, String)] = Map(
    "cm" -> ("confusionMatrix", "ConfusionMatrix"),
    "AUC" -> ("auc", "AUC"),
    "RMSE" -> ("rmse", "RMSE"),
    "MSE" -> ("mse", "MSE"),
    "Gini" -> ("gini", "Gini"),
    "AIC" -> ("aic", "AIC"),
    "pr_auc" -> ("prauc", "PRAUC"),
    "mae" -> ("mae", "MAE"),
    "rmsle" -> ("rmsle", "RMSLE"))

  def convertFromH2OToSW(parameterName: String): (String, String) = {

    val parts = parameterName.split("_")
    val capitalizedParts = parts.head +: parts.tail.map(_.capitalize)
    val regularValue = capitalizedParts.mkString
    h2oToSWExceptions.getOrElse(parameterName, (regularValue, regularValue.capitalize))
  }
}
