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

package org.apache.spark.ml.h2o.models

import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.ml.models.{H2OMOJOSettings => NewH2OMOJOSettings}
import org.apache.spark.expose.Logging

@Deprecated
case class H2OMOJOSettings(
                            predictionCol: String = NewH2OMOJOSettings.default.predictionCol,
                            detailedPredictionCol: String = NewH2OMOJOSettings.default.detailedPredictionCol,
                            withDetailedPredictionCol: Boolean = NewH2OMOJOSettings.default.withDetailedPredictionCol,
                            convertUnknownCategoricalLevelsToNa: Boolean = NewH2OMOJOSettings.default.convertUnknownCategoricalLevelsToNa,
                            convertInvalidNumbersToNa: Boolean = NewH2OMOJOSettings.default.convertInvalidNumbersToNa,
                            namedMojoOutputColumns: Boolean = NewH2OMOJOSettings.default.namedMojoOutputColumns
                          )


@Deprecated
object H2OMOJOSettings extends Logging {
  @DeprecatedMethod("ai.h2o.sparkling.ml.models.H2OMOJOSettings.default")
  def default = H2OMOJOSettings()
}
