package ai.h2o.sparkling.api.generation.common

case class Metric(swFieldName: String, swMetricName: String, h2oName: String, dataType: Class[_], comment: String)
