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

import java.io.{File, FileInputStream}

import hex.FrameSplitter
import hex.splitframe.ShuffleSplitFrame
import org.apache.spark.h2o.H2OFrame
import water.{JavaBridge, Key}
import water.fvec.{Frame, H2OFrame, UploadFileVec}
import water.parser.{ParseDataset, ParseSetup}

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

  def withLockAndUpdate[T <: Frame](fr: T)(f: T => Any): T = {
    fr.write_lock()
    f(fr)
    // Update frame in DKV
    fr.update()
    fr.unlock()
    fr
  }


  /**
    * This method updates the frame locally. Call fr.update() after it if you already have a lock or
    * consider calling it inside withLockAndUpdate method which obtains the lock, updates the frame and releases the lock
    */
  def allStringVecToCategorical[T <: Frame](fr: T): T = {
    fr.vecs().indices
      .filter(idx => fr.vec(idx).isString)
      .foreach(idx => fr.replace(idx, fr.vec(idx).toCategoricalVec).remove())
    fr
  }

  def uploadFile(file: File, modifyParserSetup: ParseSetup => ParseSetup = identity[ParseSetup]): H2OFrame ={
    val is = new FileInputStream(file.getAbsoluteFile)
    val stats = new UploadFileVec.ReadPutStats
    val uploadKey = Key.make[Frame](file.getName)
    val parsedKey = Key.make(file.getName + ".hex")
    val k = UploadFileVec.readPut(uploadKey, is, stats)
    val setup = modifyParserSetup(ParseSetup.guessSetup(Array(k), H2OFrame.defaultParserSetup()))
    val fr = JavaBridge.parse(parsedKey, k, setup)
    new H2OFrame(fr)
  }

  def uploadFiles(files: File*): H2OFrame = {
    val parsedKey = Key.make()
    val keys = files.map{ file =>
        val is = new FileInputStream(file.getAbsoluteFile)
        val stats = new UploadFileVec.ReadPutStats
        val uploadKey = Key.make[Frame](file.getName)
        UploadFileVec.readPut(uploadKey, is, stats)
      }.toArray
    val fr = ParseDataset.parse(parsedKey, keys:_*)
    new H2OFrame(fr)
  }

  def uploadFile(file: File): H2OFrame = {
    val is = new FileInputStream(file.getAbsoluteFile)
    val stats = new UploadFileVec.ReadPutStats
    val uploadKey = Key.make[Frame](file.getName)
    val parsedKey = Key.make(file.getName + ".hex")
    val k = UploadFileVec.readPut(uploadKey, is, stats)
    val fr = ParseDataset.parse(parsedKey, k)
    new H2OFrame(fr)
  }
}

object H2OFrameSupport extends H2OFrameSupport
