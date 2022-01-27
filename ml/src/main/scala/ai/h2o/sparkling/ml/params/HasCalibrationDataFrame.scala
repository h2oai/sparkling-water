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

import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.ml.params.DataFrameSerializationWrapper._

trait HasCalibrationDataFrame extends H2OAlgoParamsBase with HasDataFrameSerializer {

  private val calibrationDataFrame = new NullableDataFrameParam(
    this,
    "calibrationDataFrame",
    "Calibration frame for Platt Scaling. " +
      "To enable usage of the data frame, set the parameter calibrateModel to True.")

  setDefault(calibrationDataFrame -> null)

  def getCalibrationDataFrame(): DataFrame = $(calibrationDataFrame)

  def setCalibrationDataFrame(value: DataFrame): this.type = set(calibrationDataFrame, toWrapper(value))

  private[sparkling] def getCalibrationDataFrameParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("calibration_frame" -> convertDataFrameToH2OFrameKey(getCalibrationDataFrame()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("calibrationDataFrame" -> "calibration_frame")
  }
}
