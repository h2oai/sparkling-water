package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common._

trait AlgorithmTemplateBase extends PythonEntityTemplate {

  def generateCommonDefaultValues(defaultValuesOfCommonParameters: Map[String, Any]): String = {
    defaultValuesOfCommonParameters
      .map { case (name, value) => s"                 $name=${stringify(value)}" }
      .mkString(",\n")
  }

  def generateDefaultValuesFromExplicitFields(explicitFields: Seq[ExplicitField]): String = {
    explicitFields
      .map {
        case ExplicitField(h2oName, _, defaultValue) =>
          val swName = ParameterNameConverter.convertFromH2OToSW(h2oName)
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
}
