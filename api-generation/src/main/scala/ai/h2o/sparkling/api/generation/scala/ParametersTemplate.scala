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

object ParametersTemplate extends ScalaEntityTemplate with ParameterResolver {

  def apply(parameterSubstitutionContext: ParameterSubstitutionContext): String = {
    val h2oSchemaClassFullName = parameterSubstitutionContext.h2oSchemaClass.getName.replace('$', '.')
    val h2oParameterFullName = parameterSubstitutionContext.h2oParameterClass.getName.replace('$', '.')

    val parameters = resolveParameters(parameterSubstitutionContext)
    val imports = Seq(h2oSchemaClassFullName, h2oParameterFullName) ++
      parameters.filter(_.dataType.isEnum).map(_.dataType.fullName)
    val extraTraits = parameterSubstitutionContext.explicitFields.map(_.implementation)
    val parents = Seq(s"H2OAlgoParamsHelper[${parameterSubstitutionContext.h2oParameterClass.getSimpleName}]") ++
      extraTraits

    val entitySubstitutionContext = EntitySubstitutionContext(
      parameterSubstitutionContext.namespace,
      parameterSubstitutionContext.entityName,
      parents,
      imports)

    generateEntity(entitySubstitutionContext, "trait") {
      s"""  type H2O_SCHEMA = ${parameterSubstitutionContext.h2oSchemaClass.getSimpleName}
         |
         |  protected def paramTag = reflect.classTag[${parameterSubstitutionContext.h2oParameterClass.getSimpleName}]
         |
         |  protected def schemaTag = reflect.classTag[H2O_SCHEMA]
         |
         |  //
         |  // Parameter definitions
         |  //
         |${generateParameterDefinitions(parameters)}
         |
         |  //
         |  // Default values
         |  //
         |  setDefault(
         |${generateDefaultValues(parameters)})
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
         |  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
         |    super.getH2OAlgorithmParams() ++
         |      Map(
         |${generateH2OAssignments(parameters)})
         |  }
      """.stripMargin
    }
  }

  private def generateParameterDefinitions(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        val constructorMethod = resolveParameterContructorMethod(parameter)
        s"""  private val ${parameter.swName} = ${constructorMethod}(
           |    name = "${parameter.swName}",
           |    doc = "${parameter.comment}")""".stripMargin
      }
      .mkString("\n\n")
  }

  private def generateDefaultValues(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        val defaultValue = if (parameter.dataType.isEnum) {
          s"${parameter.dataType.name}.${parameter.defaultValue}.name()"
        } else {
          parameter.defaultValue
        }
        s"    ${parameter.swName} -> ${stringify(defaultValue)}"
      }
      .mkString(",\n")
  }

  private def stringify(value: Any): String = value match {
    case f: java.lang.Float => s"${f.toString.toLowerCase}f"
    case d: java.lang.Double => d.toString.toLowerCase
    case l: java.lang.Long => s"${l}L"
    case a: Array[_] => s"Array(${a.map(stringify).mkString(", ")})"
    case v if v == null => null
    case v => v.toString
  }

  private def generateGetters(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        s"  def get${parameter.swName.capitalize}(): ${resolveParameterType(parameter)} = $$(${parameter.swName})"
      }
      .mkString("\n\n")
  }

  private def generateSetters(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        if (parameter.dataType.isEnum) {
          s"""  def set${parameter.swName.capitalize}(value: String): this.type = {
             |    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[${parameter.dataType.name}](value)
             |    set(${parameter.swName}, validated)
             |  }
           """.stripMargin
        } else {
          s"""  def set${parameter.swName.capitalize}(value: ${resolveParameterType(parameter)}): this.type = {
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

  private def resolveParameterType(parameter: Parameter): String = {
    if (parameter.dataType.isEnum) {
      "String"
    } else if (parameter.dataType.name.endsWith("[]")) {
      s"Array[${parameter.dataType.name.stripSuffix("[]").capitalize}]"
    } else {
      parameter.dataType.name.capitalize
    }
  }

  private def resolveParameterContructorMethod(parameter: Parameter): String = {
    val rawPrefix = if (parameter.dataType.isEnum) {
      "string"
    } else if (parameter.dataType.name.endsWith("[]")) {
      s"${parameter.dataType.name.stripSuffix("[]").toLowerCase}Array"
    } else {
      parameter.dataType.name.toLowerCase
    }

    val finalPrefix = if (parameter.defaultValue == null) s"nullable${rawPrefix.capitalize}" else rawPrefix

    finalPrefix + "Param"
  }
}
