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

package ai.h2o.sparkling.api.generation.common

import water.api.API

trait ParameterResolver {
  def resolveParameters(parameterSubstitutionContext: ParameterSubstitutionContext): Seq[Parameter] = {
    val h2oSchemaClass = parameterSubstitutionContext.h2oSchemaClass
    val h2oParameterClass = parameterSubstitutionContext.h2oParameterClass
    val useV3Fields = h2oSchemaClass.getName.endsWith("V3")
    val relevantParameterNames = if (useV3Fields) {
      h2oSchemaClass
        .getField("fields")
        .get(null)
        .asInstanceOf[Array[String]]
        .toSet
    } else {
      Set.empty[String]
    }

    val h2oParameterInstance = h2oParameterClass.newInstance()
    val partialParameters =
      for (field <- h2oSchemaClass.getFields
           if !useV3Fields || relevantParameterNames.contains(field.getName)
           if field.getAnnotation(classOf[API]) != null
           if !parameterSubstitutionContext.ignoredParameters.contains(field.getName)
           if !parameterSubstitutionContext.explicitFields.map(_.h2oName).contains(field.getName))
        yield Parameter(
          ParameterNameConverter.convertFromH2OToSW(field.getName),
          field.getName,
          null, // Schema class doesn't have such information
          parameterSubstitutionContext.typeExceptions.getOrElse(field.getName, field.getType),
          field.getAnnotation(classOf[API]).help())

    val parameters = partialParameters.map { parameter =>
      parameterSubstitutionContext.explicitDefaultValues.get(parameter.h2oName) match {
        case Some(value) => parameter.copy(defaultValue = value)
        case None =>
          val value = parameterSubstitutionContext.defaultValueSource match {
            case DefaultValueSource.Getter =>
              val getter = h2oParameterClass.getMethod(parameter.h2oName)
              getter.invoke(h2oParameterInstance)
            case DefaultValueSource.Field =>
              val fieldName = parameterSubstitutionContext.defaultValueFieldPrefix + parameter.h2oName
              val field = h2oParameterClass.getField(fieldName)
              field.get(h2oParameterInstance)
          }
          parameter.copy(defaultValue = value)
      }
    }
    parameters
  }

  def resolveClassFullName(classSpecification: String): String = classSpecification.replace('$', '.')

  def resolveClassSimpleName(classSpecification: String): String = {
    resolveClassFullName(classSpecification).split('.').last
  }
}
