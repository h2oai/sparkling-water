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

  def calculate(
      dataFrame: DataFrame,
      predictionCol: String = "prediction",
      labelCol: String = "label",
      weightColOption: Option[String] = None,
      offsetColOption: Option[String] = None,
      distributionFamily: String = "AUTO"): H2ORegressionMetrics = {
    val domainFamilyEnum = DistributionFamily.valueOf(distributionFamily)
    val getMetricBuilder =
      () => new IndependentMetricBuilderRegression(DistributionFactory.getDistribution(domainFamilyEnum))

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
      offsetCol: String,
      distributionFamily: String): H2ORegressionMetrics = {
    calculate(dataFrame, predictionCol, labelCol, Option(weightCol), Option(offsetCol), distributionFamily)
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
