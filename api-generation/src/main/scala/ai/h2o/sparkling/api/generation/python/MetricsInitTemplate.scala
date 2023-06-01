package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common.{EntitySubstitutionContext, ModelMetricsSubstitutionContext}

object MetricsInitTemplate extends ((Seq[ModelMetricsSubstitutionContext]) => String) with PythonEntityTemplate {

  def apply(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    val metricClasses = metricSubstitutionContexts.map(_.entityName)
    val imports = metricClasses.map(metricClass => s"ai.h2o.sparkling.ml.metrics.$metricClass.$metricClass")

    val entitySubstitutionContext = EntitySubstitutionContext(null, null, null, imports)

    s"""${generateImports(entitySubstitutionContext)}
       |""".stripMargin
  }
}
