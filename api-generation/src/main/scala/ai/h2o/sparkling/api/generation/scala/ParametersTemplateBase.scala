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

import ai.h2o.sparkling.api.generation.common.{Parameter, TypeExceptions}
import ai.h2o.sparkling.api.generation.scala.ParametersTemplate.resolveParameterConstructorMethodType

trait ParametersTemplateBase {
  def generateParameterDefinitions(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        val constructorMethod = resolveParameterConstructorMethod(parameter.dataType, parameter.defaultValue)
        val comment = if (parameter.comment.endsWith(".")) parameter.comment else parameter.comment + "."
        s"""  protected val ${parameter.swName} = ${constructorMethod}(
           |    name = "${parameter.swName}",
           |    doc = \"\"\"$comment${generatePossibleValues(parameter)}\"\"\")""".stripMargin
      }
      .mkString("\n\n")
  }

  private def generatePossibleValues(parameter: Parameter): String = {
    val enumTypeOption = if (parameter.dataType.isEnum) {
      Some(parameter.dataType)
    } else if (parameter.dataType.isArray && parameter.dataType.getComponentType.isEnum) {
      Some(parameter.dataType.getComponentType)
    } else {
      None
    }
    enumTypeOption match {
      case Some(enumType) =>
        enumType.getEnumConstants().map(c => s"""``"${c}"``""").mkString(" Possible values are ", ", ", ".")
      case None => ""
    }
  }

  def generateGetters(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        val resolvedType = resolveParameterType(parameter.dataType)
        s"  def get${parameter.swName.capitalize}(): $resolvedType = $$(${parameter.swName})"
      }
      .mkString("\n\n")
  }

  def resolveParameterType(dataType: Class[_]): String = {
    if (dataType.isEnum || TypeExceptions.stringTypes.contains(dataType.getSimpleName())) {
      "String"
    } else if (dataType.isArray) {
      s"Array[${resolveParameterType(dataType.getComponentType)}]"
    } else if (dataType.getSimpleName == "StringPairV3") {
      "(String, String)"
    } else {
      dataType.getSimpleName.capitalize
    }
  }

  def resolveParameterConstructorMethodType(dataType: Class[_], defaultValue: Any): String = {
    if (dataType.isEnum || TypeExceptions.stringTypes.contains(dataType.getSimpleName())) {
      "string"
    } else if (dataType.isArray) {
      s"${resolveParameterConstructorMethodType(dataType.getComponentType, defaultValue)}Array"
    } else if (dataType.getSimpleName == "StringPairV3") {
      "StringPair"
    } else {
      dataType.getSimpleName.toLowerCase
    }
  }

  protected def resolveParameterConstructorMethod(dataType: Class[_], defaultValue: Any): String
}
