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

import hex.ModelMetricsBinomial.MetricBuilderBinomial
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

@MetricsDescription(
  description =
    "The class makes available all metrics that shared across all algorithms supporting binomial classification.")
class H2OBinomialMetrics(override val uid: String) extends H2OBinomialMetricsBase(uid) {

  def this() = this(Identifiable.randomUID("H2OBinomialMetrics"))
}

object H2OBinomialMetrics extends MetricCalculation {

  /**
    * The method calculates binomial metrics on a provided data frame with predictions and actual values.
    *
    * @param dataFrame A data frame with predictions and actual values
    * @param domain Array of classes representing negative and positive response. Negative class must at position 0 and
    *               positive at 1.
    * @param predictionCol      The name of prediction column. The prediction column must have the same type as
    *                           a detailed_prediction column coming from the transform method of H2OMOJOModel descendant.
    *                           Or the type must be FloatType/DoubleType where values represent probabilities of
    *                           the positive response.
    * @param labelCol           The name of label column that contains actual values.
    * @param weightColOption    The name of a weight column.
    * @param offsetColOption    The name of a offset column.
    * @return Calculated binomial metrics
    */
  def calculate(
      dataFrame: DataFrame,
      domain: Array[String],
      predictionCol: String = "detailed_prediction",
      labelCol: String = "label",
      weightColOption: Option[String] = None,
      offsetColOption: Option[String] = None): H2OBinomialMetrics = {
    validateDataFrameForMetricCalculation(dataFrame, domain, predictionCol, labelCol, offsetColOption, weightColOption)
    val getMetricBuilder = () => new MetricBuilderBinomial(domain)
    val castedLabelDF = dataFrame.withColumn(labelCol, col(labelCol) cast StringType)

    val gson =
      getMetricGson(getMetricBuilder, castedLabelDF, predictionCol, labelCol, offsetColOption, weightColOption, domain)
    val result = new H2OBinomialMetrics()
    result.setMetrics(gson, "H2OBinomialMetrics.calculate")
    result
  }

  // The method serves for call from Python API
  def calculateInternal(
      dataFrame: DataFrame,
      domain: java.util.ArrayList[String],
      predictionCol: String,
      labelCol: String,
      weightCol: String,
      offsetCol: String): H2OBinomialMetrics = {
    calculate(
      dataFrame,
      domain.toArray[String](new Array[String](0)),
      predictionCol,
      labelCol,
      Option(weightCol),
      Option(offsetCol))
  }

  // The method serves for call from R API
  def calculateInternal(
      dataFrame: DataFrame,
      domain: Array[String],
      predictionCol: String,
      labelCol: String,
      weightCol: String,
      offsetCol: String): H2OBinomialMetrics = {
    calculate(dataFrame, domain, predictionCol, labelCol, Option(weightCol), Option(offsetCol))
  }

  override protected def getPredictionValues(dataType: DataType, domain: Array[String], row: Row): Array[Double] = {
    dataType match {
      case StructType(fields)
          if fields(0).dataType == StringType && fields(1).dataType.isInstanceOf[StructType] &&
            fields(1).dataType.asInstanceOf[StructType].fields.forall(_.dataType == DoubleType) &&
            fields(1).dataType.asInstanceOf[StructType].fields.length == 2 =>
        val predictionStructure = row.getStruct(0)
        val prediction = predictionStructure.getString(0)
        val index = domain.indexOf(prediction).toDouble
        val probabilities = predictionStructure.getStruct(1)
        Array(index) ++ probabilities.toSeq.map(_.asInstanceOf[Double])
      case StructType(fields) if fields.forall(_.dataType == DoubleType) && fields.length == 2 =>
        val probabilities = row.getStruct(0)
        Array(-1.0) ++ probabilities.toSeq.map(_.asInstanceOf[Double])
      case DoubleType => probabilityToArray(row.getDouble(0))
      case FloatType => probabilityToArray(row.getFloat(0).toDouble)
    }
  }

  private def probabilityToArray(probability: Double): Array[Double] = {
    Array[Double](-1 /* unused */, 1 - probability, probability)
  }

  override protected def getActualValue(dataType: DataType, domain: Array[String], row: Row): Double = {
    val label = row.getString(1)
    domain.indexOf(label).toDouble
  }

  override protected def validateDataFrameForMetricCalculation(
      dataFrame: DataFrame,
      domain: Array[String],
      predictionCol: String,
      labelCol: String,
      offsetColOption: Option[String],
      weightColOption: Option[String]): Unit = {
    super.validateDataFrameForMetricCalculation(
      dataFrame,
      domain,
      predictionCol,
      labelCol,
      offsetColOption,
      weightColOption)
    val predictionType = dataFrame.schema.fields.find(_.name == predictionCol).get.dataType
    val isPredictionTypeValid = predictionType match {
      case StructType(fields)
          if fields(0).dataType == StringType && fields(1).dataType.isInstanceOf[StructType] &&
            fields(1).dataType.asInstanceOf[StructType].fields.forall(_.dataType == DoubleType) &&
            fields(1).dataType.asInstanceOf[StructType].fields.length == 2 =>
        true
      case StructType(fields) if fields.forall(_.dataType == DoubleType) && fields.length == 2 => true
      case DoubleType => true
      case FloatType => true
      case _ => false
    }
    if (!isPredictionTypeValid) {
      throw new IllegalArgumentException(
        s"The type of the prediction column '$predictionCol' is not valid. " +
          "The prediction column must have the same type as a detailed_prediction column coming from the transform " +
          "method of H2OMOJOModel descendant or a array type or vector of doubles. Or the type must be " +
          "FloatType/DoubleType where values represent probabilities of positive response.")
    }
  }
}
