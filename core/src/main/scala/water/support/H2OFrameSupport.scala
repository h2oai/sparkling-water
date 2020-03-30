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
package water.support

import ai.h2o.sparkling.backend.utils.H2OClientUtils
import ai.h2o.sparkling.macros.DeprecatedMethod
import org.apache.spark.expose.Logging
import water.fvec.{Frame, H2OFrame}

/**
  * Support class to ease work with H2O Frames
  */
@Deprecated
trait H2OFrameSupport extends JoinSupport with Logging {

  /**
    * Split & Shuffle H2O Frame into multiple frames according to specified ratios. The output keys need to be specified
    * in advance.
    *
    * @param fr     frame to split
    * @param keys   output keys
    * @param ratios output ratios
    * @tparam T H2O Frame Type
    * @return array of frames
    */
  @DeprecatedMethod("ai.h2o.sparkling.H2OFrame(frameKey).split(ratios)", "3.32")
  def splitFrame[T <: Frame](fr: T, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    ai.h2o.sparkling.H2OFrame(fr._key.toString).split(ratios: _*).map(fr => new H2OFrame(fr.frameId))
  }

  /**
    * Split H2O Frame into multiple frames according to specified ratios. The output keys need to be specified
    * in advance.
    *
    * @param fr     frame to split
    * @param keys   output keys
    * @param ratios output ratios
    * @tparam T H2O Frame Type
    * @return array of frames
    */
  @DeprecatedMethod("ai.h2o.sparkling.H2OFrame(frameKey).split(ratios)", "3.32")
  def split[T <: Frame](fr: T, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    ai.h2o.sparkling.H2OFrame(fr._key.toString).split(ratios: _*).map(fr => new H2OFrame(fr.frameId))
  }

  /**
    * This method should be used whenever the Frame needs to be updated. This method ensures to use proper
    * locking mechanism.
    *
    * @param fr frame to update
    * @param f  function to run on the frame
    * @tparam T H2O Frame Type
    * @return returns the updated frame
    */
  @DeprecatedMethod("ai.h2o.sparkling.backend.utils.H2OClientUtils.withLockAndUpdate", "3.32")
  def withLockAndUpdate[T <: Frame](fr: T)(f: T => Any): T = {
    H2OClientUtils.withLockAndUpdate(fr)(f)
  }

  /**
    * Convert all strings to categorical/enum values inside the given Frame.
    *
    * @param fr frame to update
    * @tparam T H2O Frame type
    * @return frame with string columns replaced by categoricals
    */
  @DeprecatedMethod("ai.h2o.sparkling.H2OFrame(frameKey).convertAllStringColumnsToCategorical(ratios)", "3.32")
  def allStringVecToCategorical[T <: Frame](fr: T): T = {
    val restFrame = ai.h2o.sparkling.H2OFrame(fr._key.toString).convertAllStringColumnsToCategorical()
    new H2OFrame(restFrame.frameId).asInstanceOf[T]
  }

  /**
    * Convert specific columns to categoricals
    *
    * @param fr         frame to update
    * @param colIndices indices of the columns to turn into categoricals
    * @tparam T H2O Frame type
    * @return frame with specified columns replaced by categoricals
    */
  @DeprecatedMethod("ai.h2o.sparkling.H2OFrame(frameKey).convertColumnsToCategorical(ratios)", "3.32")
  def columnsToCategorical[T <: Frame](fr: T, colIndices: Array[Int]): T = {
    val restFrame = ai.h2o.sparkling.H2OFrame(fr._key.toString).convertColumnsToCategorical(colIndices)
    new H2OFrame(restFrame.frameId).asInstanceOf[T]
  }

  /**
    * Convert specific columns to categoricals
    *
    * @param fr       frame to update
    * @param colNames indices of the columns to turn into categoricals
    * @tparam T H2O Frame type
    * @return frame with specified columns replaced by categoricals
    */
  @DeprecatedMethod("ai.h2o.sparkling.H2OFrame(frameKey).convertColumnsToCategorical(ratios)", "3.32")
  def columnsToCategorical[T <: Frame](fr: T, colNames: Array[String]): T = {
    val restFrame = ai.h2o.sparkling.H2OFrame(fr._key.toString).convertColumnsToCategorical(colNames)
    new H2OFrame(restFrame.frameId).asInstanceOf[T]
  }
}

@Deprecated
object H2OFrameSupport extends H2OFrameSupport
