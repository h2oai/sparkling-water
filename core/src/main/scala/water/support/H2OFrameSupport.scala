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

import hex.FrameSplitter
import hex.splitframe.ShuffleSplitFrame
import water.Key
import water.fvec.Frame

/**
  * Support class to ease work with H2O Frames
  */
trait H2OFrameSupport extends JoinSupport {

  /**
    * Split & Shuffle H2O Frame into multiple frames according to specified ratios. The output keys need to be specified
    * in advance. The order of the data is not kept.
    *
    * @param fr     frame to split
    * @param keys   output keys
    * @param ratios output ratios
    * @tparam T H2O Frame Type
    * @return array of frames
    */
  def splitFrame[T <: Frame](fr: T, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    val ks = keys.map(Key.make[Frame](_)).toArray
    val frs = ShuffleSplitFrame.shuffleSplitFrame(fr, ks, ratios.toArray, 1234567689L)
    frs
  }

  /**
    * Split H2O Frame into multiple frames according to specified ratios. The output keys need to be specified
    * in advance. This method keeps the order of the data
    *
    * @param fr     frame to split
    * @param keys   output keys
    * @param ratios output ratios
    * @tparam T H2O Frame Type
    * @return array of frames
    */
  def split[T <: Frame](fr: T, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    val ks = keys.map(Key.make[Frame](_)).toArray
    val splitter = new FrameSplitter(fr, ratios.toArray, ks, null)
    water.H2O.submitTask(splitter)
    // return results
    splitter.getResult
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
  def withLockAndUpdate[T <: Frame](fr: T)(f: T => Any): T = {
    fr.write_lock()
    f(fr)
    // Update frame in DKV
    fr.update()
    fr.unlock()
    fr
  }

  /**
    * Update the frame in DKV
    *
    * @param fr frame to update
    * @tparam T type of H2O frame
    * @return returns updated frame
    */
  def lockAndUpdate[T <: Frame](fr: T): T = {
    fr.write_lock()
    fr.update()
    fr.unlock()
    fr
  }

  /**
    * This method updates the frame locally.
    */
  /**
    * Convert all strings to categorical/enum values inside the given Frame.
    *
    * @param fr frame to update
    * @tparam T H2O Frame type
    * @return frame with string columns replaced by categoricals
    */
  def allStringVecToCategorical[T <: Frame](fr: T): T = {
    withLockAndUpdate(fr) { fr =>
      fr.vecs()
        .indices
        .filter(idx => fr.vec(idx).isString)
        .foreach(idx => fr.replace(idx, fr.vec(idx).toCategoricalVec).remove())
    }
  }

  /**
    * Convert specific columns to categoricals
    *
    * @param fr         frame to update
    * @param colIndices indices of the columns to turn into categoricals
    * @tparam T H2O Frame type
    * @return frame with specified columns replaced by categoricals
    */
  def columnsToCategorical[T <: Frame](fr: T, colIndices: Array[Int]): T = {
    withLockAndUpdate(fr) { fr =>
      colIndices.foreach(idx => fr.replace(idx, fr.vec(idx).toCategoricalVec).remove())
    }
  }

  /**
    * Convert specific columns to categoricals
    *
    * @param fr       frame to update
    * @param colNames indices of the columns to turn into categoricals
    * @tparam T H2O Frame type
    * @return frame with specified columns replaced by categoricals
    */
  def columnsToCategorical[T <: Frame](fr: T, colNames: Array[String]): T = {
    columnsToCategorical(fr, colNames.map(fr.names().indexOf(_)))
  }
}

object H2OFrameSupport extends H2OFrameSupport
