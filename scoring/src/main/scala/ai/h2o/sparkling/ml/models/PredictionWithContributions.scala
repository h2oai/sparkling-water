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

package ai.h2o.sparkling.ml.models

import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.types._

trait PredictionWithContributions {
  protected def getContributionsSchema(): DataType = MapType(StringType, FloatType, valueContainsNull = false)

  protected def convertContributionsToMap(
      wrapper: EasyPredictModelWrapper,
      contributions: Array[Float]): Map[String, Float] = {
    if (contributions == null) {
      null
    } else {
      val contributionNames = wrapper.getContributionNames()
      contributionNames.zip(contributions).toMap
    }
  }
}
