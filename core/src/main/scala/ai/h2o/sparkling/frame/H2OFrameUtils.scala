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

package ai.h2o.sparkling.frame

import java.text.MessageFormat
import java.util

import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.backends.external.RestApiUtils._
import water.api.schemas3._

object H2OFrameUtils {

  def convertAllStringVecToCategorical(frameId: String): H2OFrame = {
    val conf = H2OContext.ensure().getConf
    val fr = getFrame(conf, frameId)
    val columns = fr.columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    convertColumnsToCategorical(conf, frameId, columns)
  }

  def convertColumnsToCategorical(frameId: String, columns: Array[String]): H2OFrame = {
    val conf = H2OContext.ensure().getConf
    val endpoint = getClusterEndpoint(conf)
    val fr = getFrame(conf, frameId)
    val indices = fr.columns.map(_.name).zipWithIndex.toMap
    val selectedIndices = columns.map { name =>
      indices.getOrElse(name, throw new IllegalArgumentException(s"Column $name does not exist in the frame $frameId"))
    }
    val params = Map(
      "ast" -> MessageFormat.format(s"( assign {0} (:= {0} (as.factor (cols {0} {1})) {1} []))", frameId, util.Arrays.toString(selectedIndices)))
    )
    val rapidsFrameV3 = update[RapidsFrameV3](endpoint, "99/Rapids", conf, params)
    getFrame(conf, rapidsFrameV3.key.name)
  }

  def splitFrameToTrainAndValidationFrames(frameId: String, splitRatio: Double): Array[H2OFrame] = {
    val conf = H2OContext.ensure().getConf
    if (splitRatio >= 1.0) {
      throw new IllegalArgumentException("Split ratio must be lower than 1.0")
    }
    val endpoint = getClusterEndpoint(conf)
    val params = Map(
      "ratios" -> Array(splitRatio),
      "dataset" -> frameId
    )
    val splitFrameV3 = update[SplitFrameV3](endpoint, "3/SplitFrame", conf, params)
    splitFrameV3.destination_frames.map(frameKey => getFrame(conf, frameKey.name))
  }

}
