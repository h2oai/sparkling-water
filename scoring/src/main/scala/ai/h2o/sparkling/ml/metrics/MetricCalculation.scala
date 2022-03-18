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

import ai.h2o.sparkling.ml.models.RowConverter
import ai.h2o.sparkling.ml.utils.{DatasetShape, SchemaUtils}
import com.google.gson.{GsonBuilder, JsonObject}
import hex._
import hex.ModelMetrics.IndependentMetricBuilder
import org.apache.spark.{ExposeUtils, ml, mllib}
import org.apache.spark.sql.DataFrame
import water.api.{Schema, SchemaServer}
import water.api.schemas3._
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, StringType}

trait MetricCalculation {

  private[sparkling] def getFlattenDataFrame(dataFrame: DataFrame): DataFrame = {
    val flatDataFrame = DatasetShape.getDatasetShape(dataFrame.schema) match {
      case DatasetShape.Flat => dataFrame
      case DatasetShape.StructsOnly | DatasetShape.Nested =>
        SchemaUtils.appendFlattenedStructsToDataFrame(dataFrame, RowConverter.temporaryColumnPrefix)
    }
    flatDataFrame
  }

  private[sparkling] def validateDataFrameForMetricCalculation(
      flatDataFrame: DataFrame,
      labelCol: String,
      offsetColOption: Option[String],
      weightColOption: Option[String]): Unit = {

    if (labelCol != null && !flatDataFrame.columns.contains(labelCol)) {
      throw new IllegalArgumentException(
        s"DataFrame passed as a parameter does not contain label column '$labelCol'.")
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

  private[sparkling] def getMetricGson(
      createMetricBuilder: () => IndependentMetricBuilder[_],
      dataFrame: DataFrame,
      predictionCol: String,
      labelCol: String,
      offsetColOption: Option[String],
      weightColOption: Option[String],
      domain: Array[String]): JsonObject = {
    val flatDF = getFlattenDataFrame(dataFrame)
    val predictionType = flatDF.schema.fields.find(f => f.name == predictionCol).get.dataType
    val predictionColIndex = flatDF.schema.indexOf(predictionCol)
    val actualType = flatDF.schema.fields.find(f => f.name == labelCol).get.dataType
    val actualColIndex = flatDF.schema.indexOf(labelCol)
    val filledMetricsBuilder = flatDF.rdd
      .mapPartitions[IndependentMetricBuilder[_]] { rows =>
        val metricBuilder = createMetricBuilder()
        while (rows.hasNext) {
          val row = rows.next()
          val offset = offsetColOption match {
            case Some(offsetCol) => row.getDouble(row.fieldIndex(offsetCol))
            case None => 0.0d
          }
          val weight = weightColOption match {
            case Some(weightCol) => row.getDouble(row.fieldIndex(weightCol))
            case None => 1.0d
          }
          val prediction = predictionType match {
            case ArrayType(DoubleType, _) => row.getSeq[Double](predictionColIndex).toArray
            case ArrayType(FloatType, _) => row.getSeq[Float](predictionColIndex).map(_.toDouble).toArray
            case DoubleType => Array(row.getDouble(predictionColIndex))
            case FloatType => Array(row.getFloat(predictionColIndex).toDouble)
            case v if ExposeUtils.isMLVectorUDT(v) =>
              val vector = row.getAs[ml.linalg.Vector](predictionColIndex)
              vector.toDense.values
            case _: mllib.linalg.VectorUDT =>
              val vector = row.getAs[mllib.linalg.Vector](predictionColIndex)
              vector.toDense.values
          }
          val actualValue = actualType match {
            case StringType =>
              val label = row.getString(actualColIndex)
              domain.indexOf(label).toDouble
            case DoubleType => row.getDouble(actualColIndex)
            case FloatType => row.getFloat(actualColIndex)
          }
          metricBuilder.perRow(prediction, Array(actualValue), weight, offset)
        }
        Iterator.single(metricBuilder)
      }
      .reduce((f, s) => { f.reduce(s); f })

    val metrics = filledMetricsBuilder.makeModelMetrics()
    val schema = metricsToSchema(metrics)
    val json = schema.toJsonString
    new GsonBuilder().create().fromJson(json, classOf[JsonObject])
  }
}
