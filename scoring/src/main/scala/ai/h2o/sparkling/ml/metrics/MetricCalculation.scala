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

import com.google.gson.{GsonBuilder, JsonObject}
import hex._
import hex.ModelMetrics.IndependentMetricBuilder
import org.apache.spark.sql.{DataFrame, Row}
import water.api.{Schema, SchemaServer}
import water.api.schemas3._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, lit}

trait MetricCalculation {

  private[sparkling] def validateDataFrameForMetricCalculation(
      flatDataFrame: DataFrame,
      labelCol: String,
      offsetColOption: Option[String],
      weightColOption: Option[String]): Unit = {

    if (labelCol != null && !flatDataFrame.columns.contains(labelCol)) {
      throw new IllegalArgumentException(s"DataFrame passed as a parameter does not contain label column '$labelCol'.")
    }

    if (offsetColOption.isDefined) {
      val offsetCol = offsetColOption.get
      if (!flatDataFrame.columns.contains(offsetCol)) {
        throw new IllegalArgumentException(
          s"DataFrame passed as a parameter does not contain offset column '$offsetCol'.")
      }
    }

    if (weightColOption.isDefined) {
      val weightCol = weightColOption.get
      if (!flatDataFrame.columns.contains(weightCol)) {
        throw new IllegalArgumentException(
          s"DataFrame passed as a parameter does not contain weight column '$weightCol'.")
      }
    }
  }

  private[sparkling] def metricsToSchema(metrics: ModelMetrics): Schema[_, _] = {
    val schemas =
      MetricsCalculationTypeExtensions.SCHEMA_CLASSES.map(c =>
        Class.forName(c).getConstructor().newInstance().asInstanceOf[Schema[Nothing, Nothing]])
    schemas.foreach(SchemaServer.register)
    val schema = SchemaServer.schema(3, metrics)
    schema match {
      case s: ModelMetricsBinomialV3[ModelMetricsBinomial, _] =>
        s.fillFromImpl(metrics.asInstanceOf[ModelMetricsBinomial])
      case s: ModelMetricsMultinomialV3[ModelMetricsMultinomial, _] =>
        s.fillFromImpl(metrics.asInstanceOf[ModelMetricsMultinomial])
      case s: ModelMetricsRegressionV3[ModelMetricsRegression, _] =>
        s.fillFromImpl(metrics.asInstanceOf[ModelMetricsRegression])
    }
    schema
  }

  protected def getPredictionValues(dataType: DataType, domain: Array[String], row: Row): Array[Double]

  protected def getActualValue(dataType: DataType, domain: Array[String], row: Row): Double

  protected def getMetricGson(
      createMetricBuilder: () => IndependentMetricBuilder[_],
      dataFrame: DataFrame,
      predictionCol: String,
      labelCol: String,
      offsetColOption: Option[String],
      weightColOption: Option[String],
      domain: Array[String]): JsonObject = {
    val flatDF = dataFrame.select(col(predictionCol) as "prediction", col(labelCol) as "label", weightColOption match {
      case Some(weightCol) => col(weightCol) cast DoubleType as "weight"
      case None => lit(1.0d) as "weight"
    }, offsetColOption match {
      case Some(offsetCol) => col(offsetCol) cast DoubleType as "offset"
      case None => lit(0.0d) as "offset"
    })
    val predictionType = flatDF.schema.fields(0).dataType
    val actualType = flatDF.schema.fields(1).dataType
    val filledMetricsBuilder = flatDF.rdd
      .mapPartitions[IndependentMetricBuilder[_]] { rows =>
        val metricBuilder = createMetricBuilder()
        while (rows.hasNext) {
          val row = rows.next()
          val prediction = getPredictionValues(predictionType, domain, row)
          val actualValue: Double = getActualValue(actualType, domain, row)
          val weight = row.getDouble(2)
          val offset = row.getDouble(3)
          metricBuilder.perRow(prediction, Array(actualValue), weight, offset)
        }
        Iterator.single(metricBuilder)
      }
      .reduce((f, s) => { f.reduce(s); f })

    filledMetricsBuilder.postGlobal()
    val metrics = filledMetricsBuilder.makeModelMetrics()
    val schema = metricsToSchema(metrics)
    val json = schema.toJsonString
    new GsonBuilder().create().fromJson(json, classOf[JsonObject])
  }
}
