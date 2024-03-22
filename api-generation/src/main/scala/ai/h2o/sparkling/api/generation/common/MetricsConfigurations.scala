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

import water.api.{ModelMetricsAnomalyV3, ModelMetricsGLRMV99, ModelMetricsPCAV3}
import water.api.schemas3._

trait MetricsConfigurations {
  def metricsConfiguration: Seq[ModelMetricsSubstitutionContext] = {
    val duplicatedGLMMetrics = Seq("loglikelihood", "AIC")
    Seq(
      ModelMetricsSubstitutionContext(
        entityName = "H2OCommonMetrics",
        h2oSchemaClass = classOf[ModelMetricsBaseV3[_, _]],
        parentEntities = Seq("H2OMetrics"),
        classDescription = "The class makes available all metrics that shared across all algorithms, and ML problems." +
          " (classification, regression, dimension reduction)."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OBinomialMetrics",
        h2oSchemaClass = classOf[ModelMetricsBinomialV3[_, _]],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription =
          "The class makes available all metrics that shared across all algorithms supporting binomial classification."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OBinomialGLMMetrics",
        h2oSchemaClass = classOf[ModelMetricsBinomialGLMV3],
        parentEntities = Seq("H2OBinomialMetrics", "H2OGLMMetrics"),
        classDescription = "The class makes available all binomial metrics supported by GLM algorithm.",
        skipFields = duplicatedGLMMetrics),
      ModelMetricsSubstitutionContext(
        entityName = "H2ORegressionMetrics",
        h2oSchemaClass = classOf[ModelMetricsRegressionV3[_, _]],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription =
          "The class makes available all metrics that shared across all algorithms supporting regression."),
      ModelMetricsSubstitutionContext(
        entityName = "H2ORegressionGLMMetrics",
        h2oSchemaClass = classOf[ModelMetricsRegressionGLMV3],
        parentEntities = Seq("H2ORegressionMetrics", "H2OGLMMetrics"),
        classDescription = "The class makes available all regression metrics supported by GLM algorithm.",
        skipFields = duplicatedGLMMetrics),
      ModelMetricsSubstitutionContext(
        entityName = "H2ORegressionCoxPHMetrics",
        h2oSchemaClass = classOf[ModelMetricsRegressionCoxPHV3],
        parentEntities = Seq("H2ORegressionMetrics"),
        classDescription = "The class makes available all regression metrics supported by CoxPH algorithm."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OMultinomialMetrics",
        h2oSchemaClass = classOf[ModelMetricsMultinomialV3[_, _]],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription =
          "The class makes available all metrics that shared across all algorithms supporting multinomial classification."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OMultinomialGLMMetrics",
        h2oSchemaClass = classOf[ModelMetricsMultinomialGLMV3],
        parentEntities = Seq("H2OMultinomialMetrics", "H2OGLMMetrics"),
        classDescription = "The class makes available all multinomial metrics supported by GLM algorithm.",
        skipFields = duplicatedGLMMetrics),
      ModelMetricsSubstitutionContext(
        entityName = "H2OOrdinalMetrics",
        h2oSchemaClass = classOf[ModelMetricsOrdinalV3[_, _]],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription =
          "The class makes available all metrics that shared across all algorithms supporting ordinal regression."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OOrdinalGLMMetrics",
        h2oSchemaClass = classOf[ModelMetricsOrdinalGLMV3],
        parentEntities = Seq("H2OOrdinalMetrics", "H2OGLMMetrics"),
        classDescription = "The class makes available all ordinal metrics supported by GLM algorithm."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OAnomalyMetrics",
        h2oSchemaClass = classOf[ModelMetricsAnomalyV3],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription =
          "The class makes available all metrics that shared across all algorithms supporting anomaly detection."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OClusteringMetrics",
        h2oSchemaClass = classOf[ModelMetricsClusteringV3],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription =
          "The class makes available all metrics that shared across all algorithms supporting clustering."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OAutoEncoderMetrics",
        h2oSchemaClass = classOf[ModelMetricsAutoEncoderV3],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription = "The class provides all metrics available for ``H2OAutoEncoder``."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OGLRMMetrics",
        h2oSchemaClass = classOf[ModelMetricsGLRMV99],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription = "The class provides all metrics available for ``H2OGLRM``."),
      ModelMetricsSubstitutionContext(
        entityName = "H2OPCAMetrics",
        h2oSchemaClass = classOf[ModelMetricsPCAV3],
        parentEntities = Seq("H2OCommonMetrics"),
        classDescription = "The class provides all metrics available for ``H2OPCA``."))
  }
}
