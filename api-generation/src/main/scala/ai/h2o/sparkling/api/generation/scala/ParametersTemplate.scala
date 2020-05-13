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
  def apply(substitutionContext: ParameterSubstitutionContext): String = {
    val h2oSchemaClassFullName = substitutionContext.h2oSchemaClass.getName.replace('$', '.')
    val h2oParameterFullName = substitutionContext.h2oParameterClass.getName.replace('$', '.')
    val extraImports = Seq(h2oSchemaClassFullName, h2oParameterFullName, h2oParameterFullName + "._")
    val extraTraits = substitutionContext.explicitFields.map(_.implementation)
    val parameters = resolveParameters(substitutionContext)
    val contextWithExtraImports = substitutionContext.commonContext.copy(
      imports = substitutionContext.commonContext.imports ++ extraImports,
      inheritedEntities = substitutionContext.commonContext.inheritedEntities ++ extraTraits)

    generateEntity(contextWithExtraImports, "trait") {
      s"""  type H2O_SCHEMA = ${substitutionContext.h2oSchemaClass.getSimpleName}
         |
         |  protected def paramTag = reflect.classTag[${substitutionContext.h2oParameterClass.getSimpleName}]
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
        s"""  private val ${parameter.swName} = ${constructorMethod}("${parameter.swName}")"""
      }
      .mkString("\n")
  }

  private def generateDefaultValues(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        val defaultValue = if (parameter.dataType.isEnum) {
          s"${parameter.dataType.fullName}.${parameter.defaultValue}.name()"
        } else {
          parameter.defaultValue
        }
        s"    ${parameter.swName} -> ${stringify(defaultValue)}"
      }
      .mkString(",\n")
  }

  private def stringify(value: Any): String = value match {
    case f: java.lang.Float => s"${f}f"
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
             |    val validated = getValidatedEnumValue[${parameter.dataType.fullName}](value)
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
