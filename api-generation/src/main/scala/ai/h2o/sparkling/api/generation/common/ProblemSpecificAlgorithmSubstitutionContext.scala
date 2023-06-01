package ai.h2o.sparkling.api.generation.common

case class ProblemSpecificAlgorithmSubstitutionContext(
    entityName: String,
    parentEntityName: String,
    namespace: String,
    parentNamespace: String,
    parametersToCheck: Seq[String])
  extends SubstitutionContextBase
