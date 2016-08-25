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
package org.apache.spark.ml.spark.models.gm

import hex.ClusteringModel.ClusteringOutput
import hex._
import hex.ModelMetricsClustering.MetricBuilderClustering
import org.apache.spark.ml.spark.models.gm.GaussianMixtureModel.GaussianMixtureOutput
import org.apache.spark.mllib.clustering
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.stat.distribution
import water.codegen.CodeGeneratorPipeline
import water.util.SBPrintStream
import water.{Key, Keyed}

object GaussianMixtureModel {

  class GaussianMixtureOutput(val b: GaussianMixture) extends ClusteringOutput(b) {

    // TODO check how many of those we can set from Spark model
    var _iterations: Int = 0
    var _avg_centroids_chg: Array[Double] = Array[Double](Double.NaN)
    var _withinss: Array[Double] = null
    var _size: Array[Long] = null
    var _tot_withinss: Double = .0
    var _totss: Double = .0
    var _betweenss: Double = .0
    var _categorical_column_count: Int = 0
    var _training_time_ms: Array[Long] = Array[Long](System.currentTimeMillis)

    var _weights: Array[Double] = null
    var _mu: Array[Array[Double]] = null
    var _sigma: Array[Array[Double]] = null
    var _sigma_cols: Array[Int] = null
  }

}

class GaussianMixtureModel private[gm](val selfKey: Key[_ <: Keyed[_ <: Keyed[_ <: AnyRef]]],
                                       val parms: GaussianMixtureParameters,
                                       val output: GaussianMixtureOutput)
  extends ClusteringModel[GaussianMixtureModel, GaussianMixtureParameters, GaussianMixtureOutput](selfKey, parms, output) {

  private var weights: Array[Double] = null
  private var mg: Array[distribution.MultivariateGaussian]  = null
  private var sparkModel: clustering.GaussianMixtureModel = null

  def init() = {
    weights = _output._weights
    mg = _output
      ._sigma
      .zip(_output._sigma_cols)
      .zip(_output._mu)
      .map { case ((sig, cols), mean) =>
        new org.apache.spark.mllib.stat.distribution.MultivariateGaussian(
          Vectors.dense(mean),
          Matrices.dense(sig.length / cols, cols, sig))
      }
    sparkModel = new clustering.GaussianMixtureModel(_output._weights, mg)
  }

  override def makeMetricBuilder(domain: Array[String]): MetricBuilderClustering = {
      assert(domain == null)
    // TODO implement a proper metrics builder
      new ModelMetricsClustering.MetricBuilderClustering(_output.nfeatures, _parms._k)
  }

  override def score0(data: Array[Double], preds: Array[Double]): Array[Double] = {
    preds(0) = sparkModel.predict(Vectors.dense(data))
    preds
  }

  override protected def toJavaInit(sb: SBPrintStream, fileCtx: CodeGeneratorPipeline): SBPrintStream = {
    val sbInitialized = super.toJavaInit(sb, fileCtx)
    // TODO implement
    sbInitialized
  }

  override protected def toJavaPredictBody(bodySb: SBPrintStream,
                                           classCtx: CodeGeneratorPipeline,
                                           fileCtx: CodeGeneratorPipeline,
                                           verboseCode: Boolean) {
    // TODO implement

  }

}
