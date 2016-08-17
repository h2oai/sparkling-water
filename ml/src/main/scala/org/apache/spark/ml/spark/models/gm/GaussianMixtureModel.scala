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

  }

}

class GaussianMixtureModel private[gm](val selfKey: Key[_ <: Keyed[_ <: Keyed[_ <: AnyRef]]],
                                       val parms: GaussianMixtureParameters,
                                       val output: GaussianMixtureOutput)
  extends ClusteringModel[GaussianMixtureModel, GaussianMixtureParameters, GaussianMixtureOutput](selfKey, parms, output) {

  override def makeMetricBuilder(domain: Array[String]): MetricBuilderClustering = {
      assert(domain == null)
      new ModelMetricsClustering.MetricBuilderClustering(_output.nfeatures, _parms._k)
  }

  override def score0(data: Array[Double], preds: Array[Double]): Array[Double] = ???
}
