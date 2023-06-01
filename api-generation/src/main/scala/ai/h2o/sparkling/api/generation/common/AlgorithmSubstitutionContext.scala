package ai.h2o.sparkling.api.generation.common

case class AlgorithmSubstitutionContext(
    namespace: String,
    entityName: String,
    h2oSchemaClass: Class[_],
    algorithmType: String,
    extraInheritedEntities: Seq[String] = Seq.empty,
    constructorMethods: Boolean = true,
    specificMetricsClass: Option[String] = None)
  extends SubstitutionContextBase
