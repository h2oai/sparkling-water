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

import ai.h2o.sparkling.ml.models.H2OKMeansMOJOModel
import com.google.gson.{GsonBuilder, JsonObject}
import hex.ModelMetrics.IndependentMetricBuilder
import hex.ModelMetricsClustering.IndependentMetricBuilderClustering
import hex.genmodel.algos.kmeans.KMeansMojoModel
import hex.genmodel.attributes.ModelJsonReader
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}

trait KmeansMetricCalculation {
  self: H2OKMeansMOJOModel =>

  override private[sparkling] def makeMetricBuilder(wrapper: EasyPredictModelWrapper): IndependentMetricBuilder[_] = {
    val k = getK()
    val model = wrapper.m.asInstanceOf[KMeansMojoModel]
    val nCols = model.nfeatures()
    val modes = model._modes

    val gson = new GsonBuilder().create().fromJson(getModelDetails(), classOf[JsonObject])
    if (!gson.has("centers")) {
      throw new UnsupportedOperationException(
        "Calculation of metrics is not supported since the MOJO model doesn't have 'centers' field on model output.")
    }
    if (!gson.has("centers_std")) {
      throw new UnsupportedOperationException(
        "Calculation of metrics is not supported since the MOJO model doesn't have 'centers_std' field on model output.")
    }
    val centers = parse2dArray(gson, "centers")
    val centersStandardized = parse2dArray(gson, "centers_std")

    new IndependentMetricBuilderClustering(nCols, k, centers, centersStandardized, modes)
  }

  private def parse2dArray(obj: JsonObject, elementName: String): Array[Array[Double]] = {
    if (obj.get(elementName).isJsonNull) {
      null
    } else {
      val table = ModelJsonReader.readTable(obj, elementName)
      val result = new Array[Array[Double]](table.rows())
      var i = 0
      while (i < table.rows()) {
        val row = new Array[Double](table.columns() - 1)
        var j = 1
        while (j < table.columns()) {
          row(j-1) = table.getCell(j, i).asInstanceOf[Double]
          j += 1
        }
        result(i) = row
        i += 1
      }
      result
    }
  }

  override private[sparkling] def extractActualValues(rowData: RowData, wrapper: EasyPredictModelWrapper): Array[Float] = {
    val rawData = new Array[Double](wrapper.m.nfeatures())
    wrapper.fillRawData(rowData, rawData).map(_.toFloat)
  }
}
