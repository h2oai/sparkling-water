package ai.h2o.sparkling.api.generation.common

case class ModelMetricsSubstitutionContext(
    entityName: String,
    h2oSchemaClass: Class[_],
    parentEntities: Seq[String],
    classDescription: String)
  extends SubstitutionContextBase {

  val namespace = "ai.h2o.sparkling.ml.metrics"
}
