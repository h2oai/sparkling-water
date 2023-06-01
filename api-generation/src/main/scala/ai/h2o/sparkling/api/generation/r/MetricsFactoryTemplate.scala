package ai.h2o.sparkling.api.generation.r

import ai.h2o.sparkling.api.generation.common.ModelMetricsSubstitutionContext

object MetricsFactoryTemplate extends ((Seq[ModelMetricsSubstitutionContext]) => String) {

  def apply(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    val metricClasses = metricSubstitutionContexts.map(_.entityName)
    val imports = metricClasses.map(metricClass => s"""source(file.path("R", "${metricClass}.R"))""").mkString("\n")

    s"""$imports
       |
       |H2OMetricsFactory.fromJavaObject <- function(javaObject) {
       |  if (is.null(javaObject)) {
       |    NULL
       |${generateCases(metricSubstitutionContexts)}
       |  } else {
       |    rsparkling.H2OCommonMetrics(javaObject)
       |  }
       |}
       |""".stripMargin
  }

  private def generateCases(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    metricSubstitutionContexts
      .map { metricSubstitutionContext =>
        val metricsObjectName = metricSubstitutionContext.entityName
        s"""  } else if (invoke(invoke(javaObject, "getClass"), "getSimpleName") == "$metricsObjectName") {
           |    rsparkling.$metricsObjectName(javaObject)""".stripMargin
      }
      .mkString("\n")
  }
}
