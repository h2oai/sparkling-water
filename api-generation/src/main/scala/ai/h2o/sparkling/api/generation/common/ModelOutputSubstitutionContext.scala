package ai.h2o.sparkling.api.generation.common

case class ModelOutputSubstitutionContext(
    namespace: String,
    entityName: String,
    h2oSchemaClass: Class[_],
    ignoredOutputs: Seq[String])
  extends SubstitutionContextBase
