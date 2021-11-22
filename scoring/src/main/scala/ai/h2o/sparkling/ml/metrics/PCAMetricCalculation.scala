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

import ai.h2o.sparkling.ml.models.H2OPCAMOJOModel
import hex.ModelMetrics
import hex.ModelMetrics.IndependentMetricBuilder
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import hex.pca.ModelMetricsPCA

trait PCAMetricCalculation {
  self: H2OPCAMOJOModel =>

  override private[sparkling] def makeMetricBuilder(wrapper: EasyPredictModelWrapper): IndependentMetricBuilder[_] = {
    new SWPCAMetricBuilder()
  }

  override private[sparkling] def extractActualValues(
      rowData: RowData,
      wrapper: EasyPredictModelWrapper): Array[Double] = {
    val rawData = new Array[Double](wrapper.m.nfeatures())
    wrapper.fillRawData(rowData, rawData)
  }
}

// Builder calculates just dummy object as H2O runtime
class SWPCAMetricBuilder extends IndependentMetricBuilder[SWPCAMetricBuilder] {

  override def perRow(prediction: Array[Double], original: Array[Float]): Array[Double] = prediction

  override def makeModelMetrics(): ModelMetrics = {
    new ModelMetricsPCA(null, null, _customMetric)
  }
}
