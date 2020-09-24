package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common._
import ai.h2o.sparkling.api.generation.python.ProblemSpecificAlgorithmTemplate.{generateCommonDefaultValues, generateDefaultValues, generateDefaultValuesFromExplicitFields, generateEntity}

trait AlgorithmTemplateBase extends PythonEntityTemplate {

  def generateCommonDefaultValues(defaultValuesOfCommonParameters: Map[String, Any]): String = {
    defaultValuesOfCommonParameters
      .map { case (name, value) => s"                 $name=${stringify(value)}" }
      .mkString(",\n")
  }

  def generateDefaultValuesFromExplicitFields(explicitFields: Seq[ExplicitField]): String = {
    explicitFields
      .map {
        case ExplicitField(h2oName, _, defaultValue, swNameOption, _) =>
          val swName = swNameOption match {
            case Some(name) => name
            case None => ParameterNameConverter.convertFromH2OToSW(h2oName)
          }
          s"\n                 $swName=${stringify(defaultValue)},"
      }
      .mkString("")
  }

  def generateDefaultValues(parameters: Seq[Parameter], explicitDefaultValues: Map[String, Any]): String = {
    parameters
      .map { parameter =>
        val finalDefaultValue = stringify(explicitDefaultValues.getOrElse(parameter.h2oName, parameter.defaultValue))
        s"                 ${parameter.swName}=$finalDefaultValue"
      }
      .mkString(",\n")
  }

  def generateDeprecations(deprecatedParams: Seq[DeprecatedField]): String =
    if (deprecatedParams.isEmpty) {
      ""
    } else {
      deprecatedParams
        .map { param =>
          val version = param.version
          val name = param.sparkName
          s"""        if '$name' in kwargs:
             |            del kwargs['$name']
             |            warn("The parameter '$name' is deprecated and will be removed in the version $version.")""".stripMargin
        }
        .mkString("\n")
    }

  def generateAlgorithmClass(
      entityName: String,
      parentReferenceSource: String,
      namespace: String,
      parameters: Seq[Parameter],
      entitySubstitutionContext: EntitySubstitutionContext,
      commonSubstitutionContext: ParameterSubstitutionContext): String = {
    generateEntity(entitySubstitutionContext) {
      val kwargs = if (commonSubstitutionContext.deprecatedFields.isEmpty) "" else ",\n                 **kwargs"
      s"""    @keyword_only
         |    def __init__(self,${generateDefaultValuesFromExplicitFields(commonSubstitutionContext.explicitFields)}
         |${generateCommonDefaultValues(commonSubstitutionContext.defaultValuesOfCommonParameters)},
         |${generateDefaultValues(parameters, commonSubstitutionContext.explicitDefaultValues)}$kwargs):
         |        Initializer.load_sparkling_jar()
         |        super($parentReferenceSource, self).__init__()
         |        self._java_obj = self._new_java_obj("$namespace.$entityName", self.uid)
         |        self._setDefaultValuesFromJava()
         |        kwargs = Utils.getInputKwargs(self)
         |${generateDeprecations(commonSubstitutionContext.deprecatedFields)}
         |        if 'interactionPairs' in kwargs:
         |            warn("Interaction pairs are not supported!")
         |        self._set(**kwargs)
         |        self._transfer_params_to_java()""".stripMargin
    }
  }
}
