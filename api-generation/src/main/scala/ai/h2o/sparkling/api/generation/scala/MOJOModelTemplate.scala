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
import ai.h2o.sparkling.api.generation.scala.ParametersTemplate.resolveParameterConstructorMethodType

object MOJOModelTemplate
  extends ((AlgorithmSubstitutionContext, ParameterSubstitutionContext) => String)
  with ParametersTemplateBase
  with ScalaEntityTemplate
  with ParameterResolver {

  def apply(
      algorithmSubstitutionContext: AlgorithmSubstitutionContext,
      parameterSubstitutionContext: ParameterSubstitutionContext): String = {

    val parameters = resolveParameters(parameterSubstitutionContext)
      .filter(parameter =>
        !IgnoredParameters.ignoredInMOJOs(algorithmSubstitutionContext.entityName).contains(parameter.h2oName))

    val explicitFieldImplementations = parameterSubstitutionContext.explicitFields.flatMap(_.mojoImplementation)

    val imports = Seq(
      "ai.h2o.sparkling.ml.params.ParameterConstructorMethods",
      "hex.genmodel.MojoModel",
      "org.apache.spark.expose.Logging") ++
      explicitFieldImplementations.map(explicitField => s"ai.h2o.sparkling.ml.params.$explicitField")

    val parents = Seq(
      algorithmSubstitutionContext.algorithmType.replace("Algorithm", "MOJOModel(uid)"),
      "ParameterConstructorMethods",
      "Logging") ++ explicitFieldImplementations

    val entityName = algorithmSubstitutionContext.entityName
    val entityParameters = "(override val uid: String)"

    val entitySubstitutionContext = EntitySubstitutionContext(
      algorithmSubstitutionContext.namespace,
      algorithmSubstitutionContext.entityName,
      parents,
      imports,
      parameters = entityParameters)

    val mojoClass = generateEntity(entitySubstitutionContext, "class") {
      s"""  //
         |  // Parameter definitions
         |  //
         |${generateParameterDefinitions(parameters)}
         |
         |  //
         |  // Getters
         |  //
         |${generateGetters(parameters)}
         |
         |  override private[sparkling] def setSpecificParams(h2oMojo: MojoModel): Unit = {
         |    super.setSpecificParams(h2oMojo)
         |    try {
         |      val h2oParameters = h2oMojo._modelAttributes.getModelParameters()
         |      val h2oParametersMap = h2oParameters.map(i => i.name -> i.actual_value).toMap
         |
         |${generateParameterAssignments(parameters)}
         |    } catch {
         |      case e: Throwable => logError("An error occurred during a try to access H2O MOJO parameters.", e)
         |    }
         |  }
      """.stripMargin
    }

    val mojoObject = s"object $entityName extends H2OSpecificMOJOLoader[$entityName]"

    s"""$mojoClass
       |$mojoObject
     """.stripMargin
  }

  def generateParameterAssignments(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        val h2oName = parameter.h2oName
        val swName = parameter.swName
        val value = parameter.dataType.getSimpleName match {
          case "boolean" => "value.asInstanceOf[java.lang.Boolean].booleanValue()"
          case "byte" => "value.asInstanceOf[java.lang.Byte].byteValue()"
          case "short" => "value.asInstanceOf[java.lang.Short].shortValue()"
          case "int" => "value.asInstanceOf[java.lang.Integer].intValue()"
          case "long" => "value.asInstanceOf[java.lang.Long].longValue()"
          case "float" => "value.asInstanceOf[java.lang.Float].floatValue()"
          case "double" => "value.asInstanceOf[java.lang.Double].doubleValue()"
          case "ColSpecifierV3" =>
            "if (value == null) null " +
              "else value.asInstanceOf[hex.genmodel.attributes.parameters.ColumnSpecifier].getColumnName()"
          case _ => "value"
        }
        s"""      try {
           |        h2oParametersMap.get("$h2oName").foreach(value => set("$swName", $value))
           |      } catch {
           |        case e: Throwable => logError("An error occurred during setting up the '$swName' parameter.", e)
           |      }""".stripMargin
      }
      .mkString("\n\n")
  }

  protected def resolveParameterConstructorMethod(dataType: Class[_], defaultValue: Any): String = {
    val rawPrefix = resolveParameterConstructorMethodType(dataType, defaultValue)
    val finalPrefix = if (defaultValue == null || dataType.isEnum) s"nullable${rawPrefix.capitalize}" else rawPrefix
    finalPrefix + "Param"
  }
}
