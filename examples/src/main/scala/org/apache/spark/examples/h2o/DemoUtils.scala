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

package org.apache.spark.examples.h2o

import hex.{ModelMetrics, FrameSplitter}
import hex.splitframe.ShuffleSplitFrame
import hex.tree.gbm.GBMModel
import hex.Model
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.{SparkConf, SparkContext}
import water.fvec.{NewChunk, Frame, Chunk}
import water.parser.BufferedString
import water._

/**
 * Shared demo utility functions.
 */
object DemoUtils {
  @deprecated("Use SparkContextSupport trait", "1.3.5")
  def configure(appName:String = "Sparkling Water Demo"):SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
    conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local"))
    conf
  }

  @deprecated("Use SparkContextSupport trait", "1.3.5")
  def addFiles(sc: SparkContext, files: String*): Unit = {
    files.foreach( f => sc.addFile(f) )
  }
  @deprecated("Use toString on frame directly", "1.3.5")
  def printFrame(fr: H2OFrame): Unit = {
    new MRTask {
      override def map(cs: Array[Chunk]): Unit = {
        println ("Chunks: " + cs.mkString(","))
        for (r <- 0 until cs(0)._len) {
          for (c <- cs) {
            val vstr = new BufferedString
            if (c.vec().isString) {
              c.atStr(vstr, r)
              print(vstr.toString + ",")
            } else if (c.vec().isCategorical) {
              print(c.vec().domain()(c.at8(r).asInstanceOf[Int]) + ", ")
            } else {
              print(c.atd(r) + ", ")
            }
          }
          println()
        }
      }
    }.doAll(fr)
  }

  @deprecated("Use H2OFrame API, the method will be removed with H2O upgrade", "NA")
  def allStringVecToCategorical(hf: H2OFrame): H2OFrame = {
    hf.vecs().indices
      .filter(idx => hf.vec(idx).isString)
      .foreach(idx => hf.replace(idx, hf.vec(idx).toCategoricalVec).remove())
    // Update frame in DKV
    water.DKV.put(hf)
    // Return it
    hf
  }

  def residualPlotRCode(prediction:Frame, predCol: String, actual:Frame, actCol:String, h2oContext: H2OContext = null):String = {
    val (ip, port) = if (h2oContext != null) {
      val s = h2oContext.h2oLocalClient.split(":")
      (s(0), s(1))
    } else {
      ("127.0.0.1", "54321")
    }

    s"""# R script for residual plot
        |library(h2o)
        |h2o.init(ip="${ip}", port=${port})
        |
        |pred = h2o.getFrame("${prediction._key}")
        |act = h2o.getFrame ("${actual._key}")
        |
        |predDelay = pred$$${predCol}
        |actDelay = act$$${actCol}
        |
        |nrow(actDelay) == nrow(predDelay)
        |
        |residuals = predDelay - actDelay
        |
        |compare = cbind (as.data.frame(actDelay$$ArrDelay), as.data.frame(residuals$$predict))
        |nrow(compare)
        |plot( compare[,1:2] )
        |
      """.stripMargin
  }

  def splitFrame(df: H2OFrame, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    val ks = keys.map(Key.make[Frame](_)).toArray
    val frs = ShuffleSplitFrame.shuffleSplitFrame(df, ks, ratios.toArray, 1234567689L)
    frs
  }

  def split(df: H2OFrame, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    val ks = keys.map(Key.make[Frame](_)).toArray
    val splitter = new FrameSplitter(df, ratios.toArray, ks, null)
    water.H2O.submitTask(splitter)
    // return results
    splitter.getResult
  }

  @deprecated("Use ModelMetricsSupport trait", "1.3.5")
  def r2(model: GBMModel, fr: Frame) =  hex.ModelMetrics.getFromDKV(model, fr).asInstanceOf[hex.ModelMetricsSupervised].r2()

  @deprecated("Use ModelMetricsSupport trait", "1.3.5")
  def modelMetrics[T <: ModelMetrics, M <: Model[M,P,O], P <: hex.Model.Parameters, O <: hex.Model.Output]
                  (model: Model[M,P,O], fr: Frame) = ModelMetrics.getFromDKV(model, fr).asInstanceOf[T]

  @deprecated("Use ModelMetricsSupport trait", "1.3.5")
  def binomialMM[M <: Model[M,P,O], P <: hex.Model.Parameters, O <: hex.Model.Output]
                (model: Model[M,P,O], fr: Frame) = modelMetrics[hex.ModelMetricsBinomial,M,P,O](model, fr)

  case class R2(name:String, train:Double, test:Double, hold:Double) {
    override def toString: String =
      s"""
        |Results for $name:
        |  - R2 on train = ${train}
        |  - R2 on test  = ${test}
        |  - R2 on hold  = ${hold}
      """.stripMargin
  }
}

/** A prediction task - for given output of a binomial model and
  * threshold it creates a new vector which contains 0, 1 based on the threshold
  * probabilities produced by the model.
  *
  * @param threshold threshold to make a prediction
  *
  * FIXME: this is an ad-hoc solution. Wee need a library of these MRtasks
  */
class MakePredictions(val threshold : Double) extends MRTask[MakePredictions] {

  override def map(cs: Array[Chunk], nc: NewChunk): Unit = {
    val pred0 = cs(1)
    for (row <- 0 until pred0.len()) {
      nc.addNum(if (pred0.atd(row) < threshold) 0 else 1)
    }
  }
}


/** Transformation from double vector to log vector. */
class Log extends MRTask[Log] {
  override def map(c: Chunk, nc: NewChunk): Unit = {
    for (row <- 0 until c.len()) {
      nc.addNum(Math.log(c.atd(row)))
    }
  }
}

