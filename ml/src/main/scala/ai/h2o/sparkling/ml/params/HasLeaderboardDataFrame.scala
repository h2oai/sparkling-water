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

trait HasLeaderboardDataFrame extends H2OAlgoParamsBase with Logging {
  val uid: String

  private val leaderboardDataFrame = new NullableBigDataFrameParam(
    this,
    "leaderboardDataFrame",
    "This parameter allows the user to specify a particular data frame to use to score and rank models " +
      "on the leaderboard. This data frame will not be used for anything besides leaderboard scoring.")

  setDefault(leaderboardDataFrame -> null)

  def getLeaderboardDataFrame(): DataFrame = $(leaderboardDataFrame)

  def setLeaderboardDataFrame(value: DataFrame): this.type = set(leaderboardDataFrame, value)

  override private[ml] def getParameterDeserializationOverrides(): Map[String, Any => Any] = {
    super.getParameterDeserializationOverrides() + ("leaderboardDataFrame",
      (input: Any) => {
        if (input != null) {
          logWarning(s"A pipeline stage with uid '$uid' contained the 'leaderboardDataFrame' property " +
            "with a non-null value. The property was reset to null during the pipeline deserialization.")
        }
        null
    })

  }

  private[sparkling] def getLeaderboardDataFrameParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("leaderboard_frame" -> convertDataFrameToH2OFrameKey(getLeaderboardDataFrame()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("leaderboardDataFrame" -> "leaderboard_frame")
  }
}
