package ai.h2o.sparkling.api.generation.common

case class Parameter(swName: String, h2oName: String, defaultValue: Any, dataType: Class[_], comment: String)
