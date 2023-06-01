package ai.h2o.sparkling.api.generation.common

import water.api.{ModelMetricsAnomalyV3, ModelMetricsGLRMV99, ModelMetricsPCAV3}
import water.api.schemas3._

trait MetricsConfigurations {
  def metricsConfiguration: Seq[ModelMetricsSubstitutionContext] = {
    Seq(
      ModelMetricsSubstitutionContext(
        "H2OCommonMetrics",
        classOf[ModelMetricsBaseV3[_, _]],
        Seq("H2OMetrics"),
        "The class makes available all metrics that shared across all algorithms, and ML problems." +
          " (classification, regression, dimension reduction)."),
      ModelMetricsSubstitutionContext(
        "H2OBinomialMetrics",
        classOf[ModelMetricsBinomialV3[_, _]],
        Seq("H2OCommonMetrics"),
        "The class makes available all metrics that shared across all algorithms supporting binomial classification."),
      ModelMetricsSubstitutionContext(
        "H2OBinomialGLMMetrics",
        classOf[ModelMetricsBinomialGLMV3],
        Seq("H2OBinomialMetrics", "H2OGLMMetrics"),
        "The class makes available all binomial metrics supported by GLM algorithm."),
      ModelMetricsSubstitutionContext(
        "H2ORegressionMetrics",
        classOf[ModelMetricsRegressionV3[_, _]],
        Seq("H2OCommonMetrics"),
        "The class makes available all metrics that shared across all algorithms supporting regression."),
      ModelMetricsSubstitutionContext(
        "H2ORegressionGLMMetrics",
        classOf[ModelMetricsRegressionGLMV3],
        Seq("H2ORegressionMetrics", "H2OGLMMetrics"),
        "The class makes available all regression metrics supported by GLM algorithm."),
      ModelMetricsSubstitutionContext(
        "H2ORegressionCoxPHMetrics",
        classOf[ModelMetricsRegressionCoxPHV3],
        Seq("H2ORegressionMetrics"),
        "The class makes available all regression metrics supported by CoxPH algorithm."),
      ModelMetricsSubstitutionContext(
        "H2OMultinomialMetrics",
        classOf[ModelMetricsMultinomialV3[_, _]],
        Seq("H2OCommonMetrics"),
        "The class makes available all metrics that shared across all algorithms supporting multinomial classification."),
      ModelMetricsSubstitutionContext(
        "H2OMultinomialGLMMetrics",
        classOf[ModelMetricsMultinomialGLMV3],
        Seq("H2OMultinomialMetrics", "H2OGLMMetrics"),
        "The class makes available all multinomial metrics supported by GLM algorithm."),
      ModelMetricsSubstitutionContext(
        "H2OOrdinalMetrics",
        classOf[ModelMetricsOrdinalV3[_, _]],
        Seq("H2OCommonMetrics"),
        "The class makes available all metrics that shared across all algorithms supporting ordinal regression."),
      ModelMetricsSubstitutionContext(
        "H2OOrdinalGLMMetrics",
        classOf[ModelMetricsOrdinalGLMV3],
        Seq("H2OOrdinalMetrics", "H2OGLMMetrics"),
        "The class makes available all ordinal metrics supported by GLM algorithm."),
      ModelMetricsSubstitutionContext(
        "H2OAnomalyMetrics",
        classOf[ModelMetricsAnomalyV3],
        Seq("H2OCommonMetrics"),
        "The class makes available all metrics that shared across all algorithms supporting anomaly detection."),
      ModelMetricsSubstitutionContext(
        "H2OClusteringMetrics",
        classOf[ModelMetricsClusteringV3],
        Seq("H2OCommonMetrics"),
        "The class makes available all metrics that shared across all algorithms supporting clustering."),
      ModelMetricsSubstitutionContext(
        "H2OAutoEncoderMetrics",
        classOf[ModelMetricsAutoEncoderV3],
        Seq("H2OCommonMetrics"),
        "The class provides all metrics available for ``H2OAutoEncoder``."),
      ModelMetricsSubstitutionContext(
        "H2OGLRMMetrics",
        classOf[ModelMetricsGLRMV99],
        Seq("H2OCommonMetrics"),
        "The class provides all metrics available for ``H2OGLRM``."),
      ModelMetricsSubstitutionContext(
        "H2OPCAMetrics",
        classOf[ModelMetricsPCAV3],
        Seq("H2OCommonMetrics"),
        "The class provides all metrics available for ``H2OPCA``."))
  }
}
