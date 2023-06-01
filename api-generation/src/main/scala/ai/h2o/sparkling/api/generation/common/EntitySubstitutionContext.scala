package ai.h2o.sparkling.api.generation.common

case class EntitySubstitutionContext(
    namespace: String,
    entityName: String,
    inheritedEntities: Seq[String],
    imports: Seq[String],
    parameters: String = "",
    annotations: Seq[String] = Seq.empty)
  extends SubstitutionContextBase
