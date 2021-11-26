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

package ai.h2o.sparkling.ml.metrics

import ai.h2o.sparkling.ml.models.H2OGLRMMOJOModel
import hex.ModelMetrics.IndependentMetricBuilder
import hex.genmodel.algos.glrm.GlrmMojoModel
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import hex.glrm.ModelMetricsGLRM.IndependentGLRMModelMetricsBuilder

trait GLRMMetricCalculation {
  self: H2OGLRMMOJOModel =>

  override private[sparkling] def makeMetricBuilder(wrapper: EasyPredictModelWrapper): IndependentMetricBuilder[_] = {
    val model = wrapper.m.asInstanceOf[GlrmMojoModel]
    val k = getK()
    val permutation = model._permutation
    val imputeOriginal = getImputeOriginal()
    val ncats = model._ncats
    val nnums = model._nnums
    val normSub = model._normSub
    val normMul = model._normMul
    new IndependentGLRMModelMetricsBuilder(k, permutation, ncats, nnums, normSub, normMul, imputeOriginal)
  }

  override private[sparkling] def extractActualValues(
      rowData: RowData,
      wrapper: EasyPredictModelWrapper): Array[Double] = {
    val rawData = new Array[Double](wrapper.m.nfeatures())
    wrapper.fillRawData(rowData, rawData)
  }

  override private[sparkling] def getPrediction(
      wrapper: EasyPredictModelWrapper,
      rowData: RowData,
      offset: Double): Array[Double] = {
    val vpa = wrapper.predictDimReduction(rowData)
    vpa.reconstructed
  }
}
