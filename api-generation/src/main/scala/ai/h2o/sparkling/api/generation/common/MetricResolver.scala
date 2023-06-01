package ai.h2o.sparkling.api.generation.common

import water.api.API

trait MetricResolver {
  def resolveMetrics(substitutionContext: ModelMetricsSubstitutionContext): Seq[Metric] = {
    val h2oSchemaClass = substitutionContext.h2oSchemaClass

    val parameters =
      for (field <- h2oSchemaClass.getDeclaredFields
           if field.getAnnotation(classOf[API]) != null && !MetricFieldExceptions.ignored().contains(field.getName))
        yield {
          val (swFieldName, swMetricName) = MetricNameConverter.convertFromH2OToSW(field.getName)
          Metric(swFieldName, swMetricName, field.getName, field.getType, field.getAnnotation(classOf[API]).help())
        }
    parameters
  }
}
