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
import ai.h2o.sparkling.ml.utils.{DatasetShape, SchemaUtils}
import com.google.gson.{GsonBuilder, JsonObject}
import hex._
import hex.ModelMetrics.IndependentMetricBuilder
import hex.ModelMetricsBinomialGLM.{ModelMetricsMultinomialGLM, ModelMetricsOrdinalGLM}
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark.sql.DataFrame
import water.api.{Schema, SchemaServer}
import water.api.schemas3._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import water.Iced

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
    val (preparedDF, offsetColOption, weightColOption) = validateAndPrepareDataFrameForMetricCalculation(dataFrame)

    val filledMetricsBuilder = preparedDF.rdd
      .mapPartitions[IndependentMetricBuilder[_]] { rows =>
        val wrapper = loadEasyPredictModelWrapper()
        val model = wrapper.m
        val metricBuilder = makeMetricBuilder(wrapper)
        while (rows.hasNext) {
          val row = rows.next()
          val rowData = RowConverter.toH2ORowData(row)
          val offset = offsetColOption match {
            case Some(offsetCol) => row.getDouble(row.fieldIndex(offsetCol))
            case None => 0.0d
          }
          val weight = weightColOption match {
            case Some(weightCol) => row.getDouble(row.fieldIndex(weightCol))
            case None => 1.0d
          }
          val prediction = getPrediction(wrapper, rowData, offset)
          val actualValues = extractActualValues(rowData, wrapper)
          metricBuilder.perRow(prediction, actualValues, weight, offset)
        }
        Iterator.single(metricBuilder)
      }
      .reduce((f, s) => { f.reduce(s); f })

    val metrics = filledMetricsBuilder.makeModelMetrics()
    val schema = metricsToSchema(metrics)
    val json = schema.toJsonString
    new GsonBuilder().create().fromJson(json, classOf[JsonObject])
  }

  private[sparkling] def getPrediction(
      wrapper: EasyPredictModelWrapper,
      rowData: RowData,
      offset: Double): Array[Double] = {
    wrapper.preamble(wrapper.m.getModelCategory, rowData, offset)
  }

  private[sparkling] def metricsToSchema(metrics: ModelMetrics): Schema[_, _] = {
    val schemas =
      MetricsCalculationTypeExtensions.SCHEMA_CLASSES.map(c =>
        Class.forName(c).getConstructor().newInstance().asInstanceOf[Schema[Nothing, Nothing]])
    schemas.foreach(SchemaServer.register)
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
    throw new UnsupportedOperationException("This method is supposed to be overridden byt children classes.")
  }

  private[sparkling] def extractActualValues(rowData: RowData, wrapper: EasyPredictModelWrapper): Array[Double] = {
    throw new UnsupportedOperationException("This method is supposed to be overridden byt children classes.")
  }

  private[sparkling] def validateAndPrepareDataFrameForMetricCalculation(
      dataFrame: DataFrame): (DataFrame, Option[String], Option[String]) = {
    val flatDataFrame = DatasetShape.getDatasetShape(dataFrame.schema) match {
      case DatasetShape.Flat => dataFrame
      case DatasetShape.StructsOnly | DatasetShape.Nested =>
        SchemaUtils.appendFlattenedStructsToDataFrame(dataFrame, RowConverter.temporaryColumnPrefix)
    }

    if (hasParam("labelCol")) {
      val labelCol = getOrDefault(getParam("labelCol")).toString
      if (labelCol != null && !flatDataFrame.columns.contains(labelCol)) {
        throw new IllegalArgumentException(
          s"DataFrame passed as a parameter does not contain label column '$labelCol'.")
      }
    }
    val (offsetColCastedDF, offsetColOption) =
      if (hasParam("offsetCol") && getOrDefault(getParam("offsetCol")) != null) {
        val offsetCol = getOrDefault(getParam("offsetCol")).toString
        if (!flatDataFrame.columns.contains(offsetCol)) {
          throw new IllegalArgumentException(
            s"DataFrame passed as a parameter does not contain offset column '$offsetCol'.")
        }
        (flatDataFrame.withColumn(offsetCol, col(offsetCol).cast(DoubleType)), Some(offsetCol))
      } else {
        (flatDataFrame, None)
      }

    val weightColTuple = if (hasParam("weightCol") && getOrDefault(getParam("weightCol")) != null) {
      val weightCol = getOrDefault(getParam("weightCol")).toString
      if (!flatDataFrame.columns.contains(weightCol)) {
        throw new IllegalArgumentException(
          s"DataFrame passed as a parameter does not contain weight column '$weightCol'.")
      }
      (offsetColCastedDF.withColumn(weightCol, col(weightCol).cast(DoubleType)), offsetColOption, Some(weightCol))
    } else {
      (offsetColCastedDF, offsetColOption, None)
    }
    weightColTuple
  }

}
