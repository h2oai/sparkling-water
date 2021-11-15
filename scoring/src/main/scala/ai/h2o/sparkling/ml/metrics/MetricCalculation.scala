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

import ai.h2o.sparkling.ml.internals.H2OModelCategory
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, RowConverter}
import com.google.gson.{GsonBuilder, JsonObject}
import hex.ModelMetrics.IndependentMetricBuilder
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark.sql.DataFrame
import water.api.SchemaServer
import water.api.schemas3.{ModelMetricsBaseV3, ModelMetricsBinomialV3}

trait MetricCalculation {
  self: H2OMOJOModel =>

  /**
    * Returns an object holding all metrics of the Double type and also more complex performance information
    * calculated on a data frame passed as a parameter.
    */
  def getMetricsObject(dataFrame: DataFrame): H2OMetrics = {
    validateDataFrameForMetricCalculation(dataFrame)

    val filledMetricsBuilder = dataFrame.rdd
      .mapPartitions[IndependentMetricBuilder[_]]{ rows =>
        val wrapper = loadEasyPredictModelWrapper()
        val model = wrapper.m
        val metricBuilder = makeMetricBuilder(wrapper)
        while (rows.hasNext) {
          val row = rows.next()
          val rowData = RowConverter.toH2ORowData(row)
          val prediction = wrapper.preamble(model.getModelCategory, rowData) // TODO: offset
          val actualValues = extractActualValues(rowData, wrapper)
          metricBuilder.perRow(prediction, actualValues) // TODO: offset, weight
        }
        Iterator.single(metricBuilder)
      }
      .reduce((f, s) => {f.reduce(s.asInstanceOf); f})

    val metrics = filledMetricsBuilder.makeModelMetrics()
    val schema = SchemaServer.schema(3, metrics).asInstanceOf[ModelMetricsBaseV3[_,_]]
    schema.fillFromImpl(metrics)
    val json = schema.toJsonString
    val gson = new GsonBuilder().create().fromJson(json, classOf[JsonObject])
    val h2oMojo = unwrapMojoModel()
    val modelCategory = H2OModelCategory.fromString(getModelCategory())

    H2OMetrics.loadMetrics(gson, "realtime_metrics", h2oMojo._algoName, modelCategory, getDataFrameSerializer)
  }

  private[sparkling] def makeMetricBuilder(wrapper: EasyPredictModelWrapper): IndependentMetricBuilder[_] = {
    throw new UnsupportedOperationException("This method is supposed to be overriten byt children classes.")
  }

  private[sparkling] def extractActualValues(rowData: RowData, wrapper: EasyPredictModelWrapper): Array[Float] = {
    throw new UnsupportedOperationException("This method is supposed to be overriten byt children classes.")
  }

  private[sparkling] def validateDataFrameForMetricCalculation(dataFrame: DataFrame): Unit = {
    // TODO
  }
}
