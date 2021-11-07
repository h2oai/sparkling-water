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
import org.apache.spark.expose.Logging
import org.apache.spark.sql.DataFrame

trait HasBlendingDataFrame extends H2OAlgoParamsBase with Logging with HasDataFrameSerializer {

  val uid: String

  private val blendingDataFrame = new NonSerializableNullableDataFrameParam(
    this,
    "blendingDataFrame",
    "This parameter is used for  computing the predictions that serve as the training frame for the meta-learner." +
      " If provided, this triggers blending mode on the stacked ensemble training stage. Blending mode is faster" +
      " than cross-validating the base learners (though these ensembles may not perform as well as the Super Learner" +
      " ensemble).")

  setDefault(blendingDataFrame -> null)

  def getBlendingDataFrame(): DataFrame = $(blendingDataFrame)

  def setBlendingDataFrame(value: DataFrame): this.type = set(blendingDataFrame, value)

  private[sparkling] def getBlendingDataFrameParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("blending_frame" -> convertDataFrameToH2OFrameKey(getBlendingDataFrame()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("blendingDataFrame" -> "blending_frame")
  }
}
