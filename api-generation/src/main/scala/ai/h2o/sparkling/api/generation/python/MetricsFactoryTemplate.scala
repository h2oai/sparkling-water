package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common.{EntitySubstitutionContext, ModelMetricsSubstitutionContext}

object MetricsFactoryTemplate extends ((Seq[ModelMetricsSubstitutionContext]) => String) with PythonEntityTemplate {

  def apply(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    val metricClasses = metricSubstitutionContexts.map(_.entityName)
    val imports = Seq("py4j.java_gateway.JavaObject") ++
      metricClasses.map(metricClass => s"ai.h2o.sparkling.ml.metrics.$metricClass.$metricClass")

    val entitySubstitutionContext = EntitySubstitutionContext(
      metricSubstitutionContexts.head.namespace,
      "H2OMetricsFactory",
      inheritedEntities = Seq.empty,
      imports)

    generateEntity(entitySubstitutionContext) {
      s"""    def __init__(self, javaObject):
         |        self._java_obj = javaObject
         |
         |    @staticmethod
         |    def fromJavaObject(javaObject):
         |        if javaObject is None:
         |            return None
         |${generatePatternMatchingCases(metricSubstitutionContexts)}
         |        else:
         |            return H2OCommonMetrics(javaObject)""".stripMargin
    }
  }

  private def generatePatternMatchingCases(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    metricSubstitutionContexts
      .map { metricSubstitutionContext =>
        val metricsObjectName = metricSubstitutionContext.entityName
        s"""        elif javaObject.getClass().getSimpleName() == "$metricsObjectName":
           |            return $metricsObjectName(javaObject)""".stripMargin
      }
      .mkString("\n")
  }
}
