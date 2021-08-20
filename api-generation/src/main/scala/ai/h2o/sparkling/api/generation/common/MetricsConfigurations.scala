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

package ai.h2o.sparkling.api.generation.common

import water.api.{ModelMetricsAnomalyV3, ModelMetricsGLRMV99, ModelMetricsPCAV3, ModelMetricsSVDV99}
import water.api.schemas3._

trait MetricsConfigurations {
  def metricsConfiguration: Seq[ModelMetricsSubstitutionContext] = {
    val metrics = Seq[(String, Class[_], Seq[String])](
      ("H2OBinomialMetrics", classOf[ModelMetricsBinomialV3[_, _]], Seq("H2OMetrics")),
      ("H2OBinomialGLMMetrics", classOf[ModelMetricsBinomialGLMV3], Seq("H2OMetrics")),
      ("H2ORegressionMetrics", classOf[ModelMetricsRegressionV3[_, _]], Seq("H2OMetrics")),
      ("H2ORegressionGLMMetrics", classOf[ModelMetricsRegressionGLMV3], Seq("H2OMetrics")),
      ("H2ORegressionCoxPHMetrics", classOf[ModelMetricsRegressionCoxPHV3], Seq("H2OMetrics")),
      ("H2OMultinomialMetrics", classOf[ModelMetricsMultinomialV3[_, _]], Seq("H2OMetrics")),
      ("H2OMultinomialGLMMetrics", classOf[ModelMetricsMultinomialGLMV3], Seq("H2OMetrics")),
      ("H2OOrdinalMetrics", classOf[ModelMetricsOrdinalV3[_, _]], Seq("H2OMetrics")),
      ("H2OOrdinalGLMMetrics", classOf[ModelMetricsOrdinalGLMV3], Seq("H2OMetrics")),
      ("H2OAnomalyMetrics", classOf[ModelMetricsAnomalyV3], Seq("H2OMetrics")),
      ("H2OClusteringMetrics", classOf[ModelMetricsClusteringV3], Seq("H2OMetrics")),
      ("H2OAutoEncoderMetrics", classOf[ModelMetricsAutoEncoderV3], Seq("H2OMetrics")),
      ("H2OGLRMMetrics", classOf[ModelMetricsGLRMV99], Seq("H2OMetrics")),
      ("H2OPCAMetrics", classOf[ModelMetricsPCAV3], Seq("H2OMetrics")))


      for ((entityName, metricClass: Class[_], parents) <- metrics)
      yield ModelMetricsSubstitutionContext(namespace = "ai.h2o.sparkling.ml.metrics", entityName, metricClass, parents)
  }
}
