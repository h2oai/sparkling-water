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

import ai.h2o.sparkling.ml.models.H2OAutoEncoderMOJOModel
import hex.{ModelMetrics, ModelMetricsAutoEncoder}
import hex.ModelMetrics.IndependentMetricBuilder
import hex.ModelMetricsUnsupervised.IndependentMetricBuilderUnsupervised
import hex.genmodel.algos.deeplearning.DeeplearningMojoModel
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}

trait AutoEncoderMetricCalculation {
  self: H2OAutoEncoderMOJOModel =>

  override private[sparkling] def makeMetricBuilder(wrapper: EasyPredictModelWrapper): IndependentMetricBuilder[_] = {
    new SWAutoEncoderMetricBuilder(wrapper.m.asInstanceOf[DeeplearningMojoModel])
  }

  override private[sparkling] def extractActualValues(
      rowData: RowData,
      wrapper: EasyPredictModelWrapper): Array[Float] = {
    val rawData = new Array[Double](wrapper.m.nfeatures())
    wrapper.fillRawData(rowData, rawData).map(_.toFloat)
  }
}

class SWAutoEncoderMetricBuilder(@transient mojoModel: DeeplearningMojoModel)
  extends IndependentMetricBuilderUnsupervised[SWAutoEncoderMetricBuilder] {
  private var recError: Double = 0.0

  def this() = this(null)

  override def perRow(prediction: Array[Double], original: Array[Float]): Array[Double] = {
    recError += mojoModel.calculateReconstructionErrorPerRowData(original.map(_.toDouble), prediction)
    _count += 1
    prediction
  }

  override def reduce(mb: SWAutoEncoderMetricBuilder): Unit = {
    super.reduce(mb)
    recError += mb.recError
  }

  override def makeModelMetrics(): ModelMetrics = {
    new ModelMetricsAutoEncoder(null, null, _count, recError / _count, _customMetric)
  }
}
