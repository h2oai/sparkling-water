package ai.h2o.sparkling.api.generation.r

import ai.h2o.sparkling.api.generation.common.{EntitySubstitutionContext, Metric, MetricResolver, ModelMetricsSubstitutionContext}

object ModelMetricsTemplate
  extends (ModelMetricsSubstitutionContext => String)
  with MetricResolver
  with REntityTemplate {

  def apply(substitutionContext: ModelMetricsSubstitutionContext): String = {
    val metrics = resolveMetrics(substitutionContext)

    val imports = substitutionContext.parentEntities.diff(Seq("H2OGLMMetrics"))
    val parentEntities = imports.map(p => s"rsparkling.$p") // Renaming due to collision with H2O R API

    val entitySubstitutionContext = EntitySubstitutionContext(
      substitutionContext.namespace,
      "rsparkling." + substitutionContext.entityName, // Renaming due to collision with H2O R API
      parentEntities,
      imports)

    generateEntity(entitySubstitutionContext) {
      generateGetterMethods(metrics)
    }
  }

  private def generateGetterMethods(metrics: Seq[Metric]): String = {
    metrics
      .map { metric =>
        val methodName = s"get${metric.swMetricName}"
        val valueExtraction = s"""invoke(.self$$javaObject, "$methodName")"""
        val valueConversion = generateValueConversion(metric, valueExtraction)
        s"""    $methodName = function() { $valueConversion }"""
      }
      .mkString(",\n")
  }

  private def generateValueConversion(metric: Metric, value: String): String = metric.dataType.getSimpleName match {
    case "TwoDimTableV3" | "ConfusionMatrixV3" => s"sdf_register($value)"
    case _ => value
  }
}
