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
import hex._
import hex.ModelMetrics.IndependentMetricBuilder
import hex.ModelMetricsBinomialGLM.{ModelMetricsMultinomialGLM, ModelMetricsOrdinalGLM}
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark.sql.DataFrame
import water.api.{Schema, SchemaServer}
import water.api.schemas3._

trait MetricCalculation {
  self: H2OMOJOModel =>

  /**
    * Returns an object holding all metrics of the Double type and also more complex performance information
    * calculated on a data frame passed as a parameter.
    */
  def getMetricsObject(dataFrame: DataFrame): H2OMetrics = {
    val gson = getMetricGson(dataFrame)

    val h2oMojo = unwrapMojoModel()
    val modelCategory = H2OModelCategory.fromString(getModelCategory())

    H2OMetrics.loadMetrics(gson, "realtime_metrics", h2oMojo._algoName, modelCategory, getDataFrameSerializer)
  }

  /**
    * Returns a map of all metrics of the Double type calculated on a data frame passed as a parameter.
    */
  def getMetrics(dataFrame: DataFrame): Map[String, Double] = {
    val gson = getMetricGson(dataFrame)
    val conversionInput = new JsonObject()
    conversionInput.add("realtime_metrics", gson)

    extractMetrics(conversionInput, "realtime_metrics")
  }

  private[sparkling] def getMetricGson(dataFrame: DataFrame): JsonObject = {
    validateDataFrameForMetricCalculation(dataFrame)

    val filledMetricsBuilder = dataFrame.rdd
      .mapPartitions[IndependentMetricBuilder[_]] { rows =>
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
      .reduce((f, s) => { f.reduce(s.asInstanceOf); f })

    val metrics = filledMetricsBuilder.makeModelMetrics()
    val schema = metricsToSchema(metrics)
    val json = schema.toJsonString
    new GsonBuilder().create().fromJson(json, classOf[JsonObject])
  }

  private[sparkling] def metricsToSchema(metrics: ModelMetrics): Schema[_, _] = {
    val schema = SchemaServer.schema(3, metrics)
    schema match {
      case s: ModelMetricsBinomialGLMV3 => s.fillFromImpl(metrics.asInstanceOf[ModelMetricsBinomialGLM])
      case s: ModelMetricsBinomialV3[ModelMetricsBinomial, _] =>
        s.fillFromImpl(metrics.asInstanceOf[ModelMetricsBinomial])
      case s: ModelMetricsMultinomialGLMV3 => s.fillFromImpl(metrics.asInstanceOf[ModelMetricsMultinomialGLM])
      case s: ModelMetricsMultinomialV3[ModelMetricsMultinomial, _] =>
        s.fillFromImpl(metrics.asInstanceOf[ModelMetricsMultinomial])
      case s: ModelMetricsOrdinalGLMV3 => s.fillFromImpl(metrics.asInstanceOf[ModelMetricsOrdinalGLM])
      case s: ModelMetricsOrdinalV3[ModelMetricsOrdinal, _] => s.fillFromImpl(metrics.asInstanceOf[ModelMetricsOrdinal])
      case s: ModelMetricsRegressionCoxPHV3 => s.fillFromImpl(metrics.asInstanceOf[ModelMetricsRegressionCoxPH])
      case s: ModelMetricsRegressionGLMV3 => s.fillFromImpl(metrics.asInstanceOf[ModelMetricsRegressionGLM])
      case s: ModelMetricsRegressionV3[ModelMetricsRegression, _] =>
        s.fillFromImpl(metrics.asInstanceOf[ModelMetricsRegression])
      case s: ModelMetricsClusteringV3 => s.fillFromImpl(metrics.asInstanceOf[ModelMetricsClustering])
      case s: ModelMetricsHGLMV3[ModelMetricsHGLM, _] => s.fillFromImpl(metrics.asInstanceOf[ModelMetricsHGLM])
      case s: ModelMetricsAutoEncoderV3 => s.fillFromImpl(metrics)
      case s: ModelMetricsBaseV3[_, _] => s.fillFromImpl(metrics)
    }
    schema
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
