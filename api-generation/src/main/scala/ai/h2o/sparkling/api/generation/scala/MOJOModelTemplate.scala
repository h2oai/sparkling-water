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
  extends ((AlgorithmSubstitutionContext, ParameterSubstitutionContext, ModelOutputSubstitutionContext) => String)
  with ParametersTemplateBase
  with ScalaEntityTemplate
  with ParameterResolver
  with OutputResolver {

  def apply(
      algorithmSubstitutionContext: AlgorithmSubstitutionContext,
      parameterSubstitutionContext: ParameterSubstitutionContext,
      outputSubstitutionContext: ModelOutputSubstitutionContext): String = {

    val parameters = resolveParameters(parameterSubstitutionContext)
      .filterNot(parameter =>
        IgnoredParameters.ignoredInMOJOs(algorithmSubstitutionContext.entityName).contains(parameter.h2oName))

    val outputs = resolveOutputs(outputSubstitutionContext)
      .filterNot(output => IgnoredOutputs.all(algorithmSubstitutionContext.entityName).contains(output.h2oName))
      .filterNot(output => IgnoredOutputs.ignoredTypes(output.dataType.getSimpleName))

    val explicitFieldImplementations = parameterSubstitutionContext.explicitFields.flatMap(_.mojoImplementation) ++
      parameterSubstitutionContext.deprecatedFields.flatMap(_.mojoImplementation)

    val imports = Seq(
      "com.google.gson.JsonObject",
      "ai.h2o.sparkling.ml.params.ParameterConstructorMethods",
      "hex.genmodel.MojoModel",
      "org.apache.spark.expose.Logging",
      "ai.h2o.sparkling.utils.DataFrameSerializationWrappers._") ++
      explicitFieldImplementations.map(explicitField => s"ai.h2o.sparkling.ml.params.$explicitField") ++
      algorithmSubstitutionContext.specificMetricsClass.map(metrics => s"ai.h2o.sparkling.ml.metrics.$metrics")

    val parents = Seq(
      algorithmSubstitutionContext.algorithmType
        .replace("WithFoldColumn", "")
        .replace("Algorithm", "MOJOModel(uid)")
        .replace("Estimator", "MOJOModel")
        .replaceFirst("Base$", "MOJOBase"),
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
         |  // Output definitions
         |  //
         |${generateParameterDefinitions(outputs)}
         |
         |  //
         |  // Getters
         |  //
         |${generateGetters(parameters)}
         |
         |  //
         |  // Output Getters
         |  //
         |${generateGetters(outputs)}
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
         |
         |  override private[sparkling] def setOutputParameters(outputSection: JsonObject): Unit = {
         |${generateOutputParameterAssignments(outputs)}
         |  }
         |
         |${generateMetricsOverrides(algorithmSubstitutionContext.specificMetricsClass)}""".stripMargin
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
          case "float[]" =>
            """if (value.isInstanceOf[Array[Double]]) {
              |            value.asInstanceOf[Array[Double]].map(_.toFloat)
              |          } else {
              |            value
              |          }""".stripMargin
          case "ColSpecifierV3" =>
            "if (value == null) null " +
              "else value.asInstanceOf[hex.genmodel.attributes.parameters.ColumnSpecifier].getColumnName()"
          case _ if parameter.dataType.isArray && parameter.dataType.getComponentType.isEnum =>
            "if (value == null) null else value.asInstanceOf[Array[AnyRef]].map(_.toString)"
          case _ => "value"
        }
        s"""      try {
           |        h2oParametersMap.get("$h2oName").foreach { value =>
           |          val convertedValue = $value
           |          set("$swName", convertedValue)
           |        }
           |      } catch {
           |        case e: Throwable =>
           |          logWarning("An error occurred during setting up the '$swName' parameter. The method " +
           |          "get${swName.capitalize}() on the MOJO model object won't be able to provide the actual value.", e)
           |      }""".stripMargin
      }
      .mkString("\n\n")
  }

  def generateOutputParameterAssignments(outputs: Seq[Parameter]): String = {
    outputs
      .map { output =>
        val h2oName = output.h2oName
        val swName = output.swName
        val value = output.dataType.getSimpleName match {
          case "boolean" => s"""outputSection.get("$h2oName").getAsBoolean()"""
          case "byte" => s"""outputSection.get("$h2oName").getAsByte()"""
          case "short" => s"""outputSection.get("$h2oName").getAsShort()"""
          case "int" => s"""outputSection.get("$h2oName").getAsInt()"""
          case "long" => s"""outputSection.get("$h2oName").getAsLong()"""
          case "float" => s"""outputSection.get("$h2oName").getAsFloat()"""
          case "double" => s"""outputSection.get("$h2oName").getAsDouble()"""
          case "double[]" => s"""jsonFieldToDoubleArray(outputSection, "$h2oName")"""
          case "TwoDimTableV3" => s"""toWrapper(jsonFieldToDataFrame(outputSection, "$h2oName"))"""
        }
        s"""      if (outputSection.has("$h2oName")) {
           |        try {
           |          val extractedValue = $value
           |          set("$swName", extractedValue)
           |        } catch {
           |          case e: Throwable if System.getProperty("spark.testing", "false") != "true" =>
           |            logWarning("An error occurred during setting up the '$swName' parameter. The method " +
           |              "get${swName.capitalize}() on the MOJO model object won't be able to provide the actual value.", e)
           |        }
           |      } else if (System.getProperty("spark.testing", "false") == "true") {
           |        throw new AssertionError("The output field '$h2oName' in does not exist.")
           |      }""".stripMargin
      }
      .mkString("\n\n")
  }

  protected def resolveParameterConstructorMethod(dataType: Class[_], defaultValue: Any): String = {
    val rawPrefix = resolveParameterConstructorMethodType(dataType, defaultValue)
    val finalPrefix = if (defaultValue == null || dataType.isEnum) s"nullable${rawPrefix.capitalize}" else rawPrefix
    finalPrefix + "Param"
  }

  protected def generateMetricsOverrides(metricsClass: Option[String]): String = metricsClass match {
    case None => ""
    case Some(metrics) =>
      s"""  override def getTrainingMetricsObject(): $metrics = {
         |    val value = super.getTrainingMetricsObject()
         |    if (value == null) null else value.asInstanceOf[$metrics]
         |  }
         |
         |  override def getValidationMetricsObject(): $metrics = {
         |    val value = super.getValidationMetricsObject()
         |    if (value == null) null else value.asInstanceOf[$metrics]
         |  }
         |
         |  override def getCrossValidationMetricsObject(): $metrics = {
         |    val value = super.getCrossValidationMetricsObject()
         |    if (value == null) null else value.asInstanceOf[$metrics]
         |  }""".stripMargin
  }
}
