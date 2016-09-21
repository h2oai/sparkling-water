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

trait H2OFrameSupport extends JoinSupport {

  def splitFrame[T <: Frame](fr: T, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    val ks = keys.map(Key.make[Frame](_)).toArray
    val frs = ShuffleSplitFrame.shuffleSplitFrame(fr, ks, ratios.toArray, 1234567689L)
    frs
  }

  def split[T <: Frame](fr: T, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    val ks = keys.map(Key.make[Frame](_)).toArray
    val splitter = new FrameSplitter(fr, ratios.toArray, ks, null)
    water.H2O.submitTask(splitter)
    // return results
    splitter.getResult
  }

  def allStringVecToCategorical[T <: Frame](fr: T): T = {
    fr.vecs().indices
      .filter(idx => fr.vec(idx).isString)
      .foreach(idx => fr.replace(idx, fr.vec(idx).toCategoricalVec).remove())
    // Update frame in DKV
    water.DKV.put(fr)
    // Return it
    fr
  }
}

object H2OFrameSupport extends H2OFrameSupport
