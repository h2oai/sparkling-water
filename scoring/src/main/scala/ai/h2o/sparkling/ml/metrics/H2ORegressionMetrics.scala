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

import hex.DistributionFactory
import hex.ModelMetricsRegression.IndependentMetricBuilderRegression
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

@MetricsDescription(
  description = "The class makes available all metrics that shared across all algorithms supporting regression.")
class H2ORegressionMetrics(override val uid: String) extends H2ORegressionMetricsBase(uid) {

  def this() = this(Identifiable.randomUID("H2ORegressionMetrics"))
}

object H2ORegressionMetrics extends MetricCalculation {

  /**
    * The method calculates regression metrics on a provided data frame with predictions and actual values.
    *
    * @param dataFrame A data frame with predictions and actual values
    * @param predictionCol      The name of prediction column. The prediction column must have the same type as
    *                           a detailed_prediction column coming from the transform method of H2OMOJOModel descendant or
    *                           it must be of DoubleType or FloatType.
    * @param labelCol           The name of label column that contains actual values.
    * @param weightColOption    The name of a weight column.
    * @param offsetColOption    The name of a offset column.
    * @return Calculated regression metrics
    */
  def calculate(
      dataFrame: DataFrame,
      predictionCol: String = "prediction",
      labelCol: String = "label",
      weightColOption: Option[String] = None,
      offsetColOption: Option[String] = None): H2ORegressionMetrics = {
    val getMetricBuilder =
      () => new IndependentMetricBuilderRegression(DistributionFactory.getDistribution(DistributionFamily.AUTO))

    val gson =
      getMetricGson(getMetricBuilder, dataFrame, predictionCol, labelCol, offsetColOption, weightColOption, null)
    val result = new H2ORegressionMetrics()
    result.setMetrics(gson, "H2ORegressionMetrics.calculate")
    result
  }

  def calculate(
      dataFrame: DataFrame,
      predictionCol: String,
      labelCol: String,
      weightCol: String,
      offsetCol: String): H2ORegressionMetrics = {
    calculate(dataFrame, predictionCol, labelCol, Option(weightCol), Option(offsetCol))
  }

  override protected def getPredictionValues(dataType: DataType, domain: Array[String], row: Row): Array[Double] = {
    dataType match {
      case StructType(fields) if fields.head.dataType == DoubleType => Array(row.getStruct(0).getDouble(0))
      case DoubleType => Array(row.getDouble(0))
      case FloatType => Array(row.getFloat(0).toDouble)
    }
  }

  override protected def getActualValue(dataType: DataType, domain: Array[String], row: Row): Double = dataType match {
    case DoubleType => row.getDouble(1)
    case FloatType => row.getFloat(1).toDouble
    case LongType => row.getLong(1).toDouble
    case IntegerType => row.getInt(1).toDouble
    case ShortType => row.getShort(1).toDouble
    case ByteType => row.getByte(1).toDouble
  }
}
