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

package ai.h2o.sparkling.ml.metrics

import ai.h2o.sparkling.ml.models.H2OMOJOModelUtils
import ai.h2o.sparkling.ml.params.ParameterConstructorMethods
import com.google.gson.JsonObject
import org.apache.spark.expose.Logging
import org.apache.spark.ml.param.ParamMap

trait H2OMetrics extends ParameterConstructorMethods with H2OMOJOModelUtils with Logging {
  /// Getters
  /**
    * The time in mS since the epoch for the start of this scoring run.
    */
  def getScoringTime(): Long

  /**
    * The Mean Squared Error of the prediction for this scoring run.
    */
  def getMSE(): Double

  /**
    * The Root Mean Squared Error of the prediction for this scoring run.
    */
  def getRMSE(): Double

  /**
    * Number of observations.
    */
  def getNobs(): Long

  /**
    * Name of custom metric.
    */
  def getCustomMetricName(): String

  /**
    * Value of custom metric.
    */
  def getCustomMetricValue(): Double

  def setMetrics(json: JsonObject, context: String): Unit

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object H2OMetrics {
  def loadMetrics(json: JsonObject): H2OMetrics = {
    ??? // TODO
  }
}
