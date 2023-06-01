package ai.h2o.sparkling.api.generation.common

case class ParameterSubstitutionContext(
    namespace: String,
    entityName: String,
    h2oSchemaClass: Class[_],
    h2oParameterClass: Class[_],
    ignoredParameters: Seq[String],
    explicitFields: Seq[ExplicitField],
    deprecatedFields: Seq[DeprecatedField],
    explicitDefaultValues: Map[String, Any],
    typeExceptions: Map[String, Class[_]],
    defaultValueFieldPrefix: String = "_",
    defaultValueSource: DefaultValueSource.DefaultValueSource,
    defaultValuesOfCommonParameters: Map[String, Any],
    generateParamTag: Boolean)
  extends SubstitutionContextBase

case class ExplicitField(
    h2oName: String,
    implementation: String,
    defaultValue: Any,
    sparkName: Option[String] = None,
    mojoImplementation: Option[String] = None)

case class DeprecatedField(
    h2oName: String,
    implementation: String,
    sparkName: String,
    version: String,
    replacement: Option[String] = None,
    mojoImplementation: Option[String] = None)

object DefaultValueSource extends Enumeration {
  type DefaultValueSource = Value
  val Field, Getter = Value
}
