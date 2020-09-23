/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.h2o.sparkling.api.generation.scala

import ai.h2o.sparkling.api.generation.common._

object ParametersTemplate
  extends ((ParameterSubstitutionContext) => String)
  with ParametersTemplateBase
  with ScalaEntityTemplate
  with ParameterResolver {

  def apply(parameterSubstitutionContext: ParameterSubstitutionContext): String = {
    val h2oParameterFullName = parameterSubstitutionContext.h2oParameterClass.getCanonicalName

    val parameters = resolveParameters(parameterSubstitutionContext)
    val imports = Seq(h2oParameterFullName, "ai.h2o.sparkling.H2OFrame") ++
      parameters.filter(_.dataType.isEnum).map(_.dataType.getCanonicalName)
    val parents = Seq("H2OAlgoParamsBase") ++
      parameterSubstitutionContext.explicitFields.map(_.implementation) ++
      parameterSubstitutionContext.deprecatedFields.map(_.implementation)

    val entitySubstitutionContext = EntitySubstitutionContext(
      parameterSubstitutionContext.namespace,
      parameterSubstitutionContext.entityName,
      parents,
      imports)

    generateEntity(entitySubstitutionContext, "trait") {
      s"""${generateParamTag(parameterSubstitutionContext)}
         |  //
         |  // Parameter definitions
         |  //
         |${generateParameterDefinitions(parameters)}
         |
         |  //
         |  // Default values
         |  //
         |  setDefault(
         |${generateDefaultValues(parameters, parameterSubstitutionContext.explicitDefaultValues)})
         |
         |  //
         |  // Getters
         |  //
         |${generateGetters(parameters)}
         |
         |  //
         |  // Setters
         |  //
         |${generateSetters(parameters)}
         |
         |  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
         |    super.getH2OAlgorithmParams(trainingFrame) ++ get${parameterSubstitutionContext.entityName}()
         |  }
         |
         |  private[sparkling] def get${parameterSubstitutionContext.entityName}(): Map[String, Any] = {
         |      Map(
         |${generateH2OAssignments(parameters)})
         |  }
         |
         |  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
         |    super.getSWtoH2OParamNameMap() ++
         |      Map(
         |${generateSWToH2OParamNameAssociations(parameters)})
         |  }
      """.stripMargin
    }
  }

  private def generateParamTag(parameterSubstitutionContext: ParameterSubstitutionContext): String = {
    if (parameterSubstitutionContext.generateParamTag) {
      s"  protected def paramTag = reflect.classTag[${parameterSubstitutionContext.h2oParameterClass.getSimpleName}]\n"
    } else {
      ""
    }
  }

  private def generateDefaultValues(parameters: Seq[Parameter], explicitDefaultValues: Map[String, Any]): String = {
    parameters
      .map { parameter =>
        val defaultValue = if (parameter.dataType.isEnum) {
          s"${parameter.dataType.getSimpleName}.${parameter.defaultValue}.name()"
        } else {
          stringify(explicitDefaultValues.getOrElse(parameter.h2oName, parameter.defaultValue))
        }
        s"    ${parameter.swName} -> $defaultValue"
      }
      .mkString(",\n")
  }

  private def generateSetters(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        if (parameter.dataType.isEnum) {
          s"""  def set${parameter.swName.capitalize}(value: String): this.type = {
             |    val validated = EnumParamValidator.getValidatedEnumValue[${parameter.dataType.getSimpleName}](value)
             |    set(${parameter.swName}, validated)
             |  }
           """.stripMargin
        } else if (parameter.dataType.isArray && parameter.dataType.getComponentType.isEnum) {
          val enumType = parameter.dataType.getComponentType.getCanonicalName
          s"""  def set${parameter.swName.capitalize}(value: Array[String]): this.type = {
             |    val validated = EnumParamValidator.getValidatedEnumValues[$enumType](value, nullEnabled = true)
             |    set(${parameter.swName}, validated)
             |  }
           """.stripMargin
        } else {
          s"""  def set${parameter.swName.capitalize}(value: ${resolveParameterType(parameter.dataType)}): this.type = {
             |    set(${parameter.swName}, value)
             |  }
           """.stripMargin
        }
      }
      .mkString("\n")
  }

  private def generateH2OAssignments(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        s"""        "${parameter.h2oName}" -> get${parameter.swName.capitalize}()"""
      }
      .mkString(",\n")
  }

  private def generateSWToH2OParamNameAssociations(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        s"""        "${parameter.swName}" -> "${parameter.h2oName}""""
      }
      .mkString(",\n")
  }

  override protected def resolveParameterConstructorMethod(dataType: Class[_], defaultValue: Any): String = {
    val rawPrefix = resolveParameterConstructorMethodType(dataType, defaultValue)
    val finalPrefix = if (defaultValue == null) s"nullable${rawPrefix.capitalize}" else rawPrefix
    finalPrefix + "Param"
  }
}
