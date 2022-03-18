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

import ai.h2o.sparkling.ml.metrics.H2OBinomialMetrics.getMetricGson
import hex.DistributionFactory
import hex.ModelMetricsRegression.IndependentMetricBuilderRegression
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.sql.DataFrame

object H2ORegressionMetrics {

  def calculate(
      dataFrame: DataFrame,
      predictionCol: String = "prediction",
      labelCol: String = "label",
      weightColOption: Option[String] = None,
      offsetColOption: Option[String] = None,
      distributionFamily: String = "AUTO"): H2ORegressionMetrics = {
    val domainFamilyEnum = DistributionFamily.valueOf(distributionFamily)
    val distribution= DistributionFactory.getDistribution(domainFamilyEnum)
    val getMetricBuilder = () => new IndependentMetricBuilderRegression[_](distribution)

    val gson = getMetricGson(
      getMetricBuilder,
      dataFrame,
      predictionCol,
      labelCol,
      offsetColOption,
      weightColOption,
      null)
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
    calculate(
      dataFrame,
      predictionCol,
      labelCol,
      Option(weightCol),
      Option(offsetCol),
      distributionFamily)
  }
}
