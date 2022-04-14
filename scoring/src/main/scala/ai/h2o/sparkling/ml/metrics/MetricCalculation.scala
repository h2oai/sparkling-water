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
import hex.ModelMetrics.MetricBuilder
import org.apache.spark.sql.{DataFrame, Row}
import water.api.{Schema, SchemaServer}
import water.api.schemas3._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, lit}

trait MetricCalculation {

  protected def validateDataFrameForMetricCalculation(
      dataFrame: DataFrame,
      domain: Array[String],
      predictionCol: String,
      labelCol: String,
      offsetColOption: Option[String],
      weightColOption: Option[String]): Unit = {

    if (predictionCol != null && !dataFrame.columns.contains(predictionCol)) {
      throw new IllegalArgumentException(
        s"DataFrame passed as a parameter does not contain prediction column '$predictionCol'.")
    }

    if (labelCol != null && !dataFrame.columns.contains(labelCol)) {
      throw new IllegalArgumentException(s"DataFrame passed as a parameter does not contain label column '$labelCol'.")
    }

    if (offsetColOption.isDefined) {
      val offsetCol = offsetColOption.get
      if (!dataFrame.columns.contains(offsetCol)) {
        throw new IllegalArgumentException(
          s"DataFrame passed as a parameter does not contain offset column '$offsetCol'.")
      }
      val offsetType = dataFrame.schema.fields.find(_.name == offsetCol).get.dataType
      if (!offsetType.isInstanceOf[NumericType]) {
        throw new IllegalArgumentException(s"The offset column '$offsetCol' must be a numeric type.")
      }
    }

    if (weightColOption.isDefined) {
      val weightCol = weightColOption.get
      if (!dataFrame.columns.contains(weightCol)) {
        throw new IllegalArgumentException(
          s"DataFrame passed as a parameter does not contain weight column '$weightCol'.")
      }
      val weightType = dataFrame.schema.fields.find(_.name == weightCol).get.dataType
      if (!weightType.isInstanceOf[NumericType]) {
        throw new IllegalArgumentException(s"The weight column '$weightType' must be a numeric type.")
      }
    }
  }

  private def metricsToSchema(metrics: ModelMetrics): Schema[_, _] = {
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
      createMetricBuilder: () => MetricBuilder[_],
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
      .mapPartitions[MetricBuilder[_]] { rows =>
        val metricBuilder = createMetricBuilder()
        while (rows.hasNext) {
          val row = rows.next()
          val prediction = getPredictionValues(predictionType, domain, row)
          val actualValue: Double = getActualValue(actualType, domain, row)
          val weight = row.getDouble(2)
          val offset = row.getDouble(3)
          metricBuilder.perRow(prediction, Array(actualValue.toFloat), weight, offset, null)
        }
        Iterator.single(metricBuilder)
      }
      .reduce((f, s) => { f.reduce(s); f })

    filledMetricsBuilder.postGlobal()

    // Setting parameters of makeModelMetrics to null since they are required only by H2O runtime
    val model = null
    val frame = null
    val adaptedFrame = null
    val predictions  = null
    val metrics = filledMetricsBuilder.makeModelMetrics(model, frame, adaptedFrame, predictions)

    val schema = metricsToSchema(metrics)
    val json = schema.toJsonString
    new GsonBuilder().create().fromJson(json, classOf[JsonObject])
  }
}
