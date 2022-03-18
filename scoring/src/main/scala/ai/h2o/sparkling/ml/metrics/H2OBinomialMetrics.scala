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

import hex.ModelMetricsBinomial.IndependentMetricBuilderBinomial
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.sql.DataFrame

object H2OBinomialMetrics extends MetricCalculation {

  def calculate(
      dataFrame: DataFrame,
      domain: Array[String],
      predictionProbabilitiesCol: String = "detailed_prediction.probabilities",
      labelCol: String = "label",
      weightColOption: Option[String] = None,
      offsetColOption: Option[String] = None,
      distributionFamily: String = "AUTO"): H2OBinomialMetrics = {
    val domainFamilyEnum = DistributionFamily.valueOf(distributionFamily)
    val getMetricBuilder = () => new IndependentMetricBuilderBinomial[_](domain, domainFamilyEnum)

    val gson = getMetricGson(
      getMetricBuilder,
      dataFrame,
      predictionProbabilitiesCol,
      labelCol,
      offsetColOption,
      weightColOption,
      domain)
    val result = new H2OBinomialMetrics()
    result.setMetrics(gson, "H2OBinomialMetrics.calculate")
    result
  }

  def calculate(
      dataFrame: DataFrame,
      domain: Array[String],
      predictionProbabilitiesCol: String,
      labelCol: String,
      weightCol: String,
      offsetCol: String,
      distributionFamily: String): Unit = {
    calculate(
      dataFrame,
      domain,
      predictionProbabilitiesCol,
      labelCol,
      Option(weightCol),
      Option(offsetCol),
      distributionFamily)
  }
}
