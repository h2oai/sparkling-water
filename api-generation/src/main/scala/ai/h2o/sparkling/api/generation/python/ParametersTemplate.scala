package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common._

object ParametersTemplate
  extends ((ParameterSubstitutionContext) => String)
  with PythonEntityTemplate
  with ParameterResolver {

  def apply(parameterSubstitutionContext: ParameterSubstitutionContext): String = {
    val h2oParameterFullName = parameterSubstitutionContext.h2oParameterClass.getCanonicalName
    val explicitFields = parameterSubstitutionContext.explicitFields
    val deprecatedFields = parameterSubstitutionContext.deprecatedFields

    val parameters = resolveParameters(parameterSubstitutionContext)
    val imports = Seq("pyspark.ml.param.*", "ai.h2o.sparkling.ml.params.H2OTypeConverters.H2OTypeConverters") ++
      explicitFields.map(ef => s"ai.h2o.sparkling.ml.params.${ef.implementation}.${ef.implementation}") ++
      deprecatedFields.map(df => s"ai.h2o.sparkling.ml.params.${df.implementation}.${df.implementation}")

    val parents = explicitFields.map(_.implementation) ++ deprecatedFields.map(_.implementation) ++ Seq("Params")

    val entitySubstitutionContext = EntitySubstitutionContext(
      parameterSubstitutionContext.namespace,
      parameterSubstitutionContext.entityName,
      parents,
      imports)

    generateEntity(entitySubstitutionContext) {
      if (parameters.isEmpty) {
        "    pass"
      } else {
        s"""    ##
           |    # Param definitions
           |    ##
           |${generateParameterDefinitions(parameters)}
           |
           |    ##
           |    # Getters
           |    ##
           |${generateGetters(parameters)}
           |
           |    ##
           |    # Setters
           |    ##
           |${generateSetters(parameters)}""".stripMargin
      }
    }
  }

  private def generateParameterDefinitions(parameters: Seq[Parameter]): String = {
    val tripleQuotes = "\"\"\""
    parameters
      .map { parameter =>
        val converter = resolveConverter(parameter.dataType, parameter.defaultValue)
        s"""    ${parameter.swName} = Param(
           |        Params._dummy(),
           |        "${parameter.swName}",
           |        $tripleQuotes${parameter.comment}$tripleQuotes,
           |        $converter)""".stripMargin
      }
      .mkString("\n\n")
  }

  private def generateGetters(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        s"""|    def get${parameter.swName.capitalize}(self):
            |        return self.getOrDefault(self.${parameter.swName})""".stripMargin
      }
      .mkString("\n\n")
  }

  private def generateSetters(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        s"""|    def set${parameter.swName.capitalize}(self, value):
            |        return self._set(${parameter.swName}=value)""".stripMargin
      }
      .mkString("\n\n")
  }

  private def resolveConverterType(dataType: Class[_], defaultValue: Any): String = {
    if (dataType.isEnum) {
      "EnumString"
    } else if (dataType.isArray) {
      s"List${resolveConverterType(dataType.getComponentType, defaultValue)}"
    } else if (dataType.getSimpleName == "StringPairV3") {
      "PairString"
    } else if (TypeExceptions.stringTypes.contains(dataType.getSimpleName)) {
      "String"
    } else {
      dataType.getSimpleName.capitalize match {
        case "Double" => "Float"
        case "Long" => "Int"
        case t => t
      }
    }
  }

  private def resolveConverterParameter(dataType: Class[_]): String = {
    if (dataType.isEnum) {
      s""""${dataType.getName}""""
    } else if (dataType.isArray) {
      resolveConverterParameter(dataType.getComponentType)
    } else {
      ""
    }
  }

  private def resolveConverter(dataType: Class[_], defaultValue: Any): String = {
    val rawType = resolveConverterType(dataType, defaultValue)
    val finalType = if (defaultValue == null) s"Nullable$rawType" else rawType
    s"H2OTypeConverters.to${finalType}(${resolveConverterParameter(dataType)})"
  }
}
