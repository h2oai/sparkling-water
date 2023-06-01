package ai.h2o.sparkling.doc.generation

object MetricsTocTreeTemplate {
  def apply(metrics: Seq[Class[_]]): String = {
    val items = metrics.map(metric => s"   metrics_${metric.getSimpleName}").mkString("\n")
    s""".. _metrics:
       |
       |Metric Classes
       |==============
       |
       |.. toctree::
       |   :maxdepth: 1
       |
       |$items
       |""".stripMargin
  }
}
