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

package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common._

object MOJOModelTemplate
  extends ((AlgorithmSubstitutionContext, ParameterSubstitutionContext) => String)
  with PythonEntityTemplate
  with ParameterResolver {

  def apply(
      algorithmSubstitutionContext: AlgorithmSubstitutionContext,
      parameterSubstitutionContext: ParameterSubstitutionContext): String = {
    val parameters = resolveParameters(parameterSubstitutionContext)
      .filter(parameter =>
        !IgnoredParameters.ignoredInMOJOs(algorithmSubstitutionContext.entityName).contains(parameter.h2oName))
    val entityName = algorithmSubstitutionContext.entityName
    val namespace = algorithmSubstitutionContext.namespace
    val algorithmType = algorithmSubstitutionContext.algorithmType.replace("Algorithm", "MOJOModelParams")
    val explicitFields = parameterSubstitutionContext.explicitFields.flatMap(_.mojoImplementation)
    val parents = Seq(algorithmType) ++ explicitFields

    val imports = Seq(
      s"ai.h2o.sparkling.ml.params.H2OMOJOModelParams.${algorithmType}",
      "pyspark.ml.util._jvm",
      "ai.h2o.sparkling.Initializer.Initializer",
      "ai.h2o.sparkling.ml.models.H2OMOJOSettings.H2OMOJOSettings",
      "ai.h2o.sparkling.ml.params.H2OTypeConverters.H2OTypeConverters") ++
      explicitFields.map(field => s"ai.h2o.sparkling.ml.params.$field.$field")

    val entitySubstitutionContext = EntitySubstitutionContext(namespace, entityName, parents, imports)

    generateEntity(entitySubstitutionContext) {
      s"""    @staticmethod
         |    def createFromMojo(pathToMojo, settings=H2OMOJOSettings.default()):
         |        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
         |        Initializer.load_sparkling_jar()
         |        javaModel = _jvm().ai.h2o.sparkling.ml.models.${entityName}.createFromMojo(pathToMojo, settings.toJavaObject())
         |        return ${entityName}(javaModel)
         |""".stripMargin ++
        generateGetterMethods(parameters)
    }
  }

  private def generateGetterMethods(parameters: Seq[Parameter]): String = {
    parameters
      .map { parameter =>
        val parameterName = parameter.swName.capitalize
        val valueConversion = generateValueConversion(parameter)
        s"""
         |    def get${parameterName}(self):
         |        value = self._java_obj.get${parameterName}()
         |        return $valueConversion""".stripMargin
      }
      .mkString("\n\n")
  }

  private def generateValueConversion(parameter: Parameter): String = parameter.dataType match {
    case x if x.isArray => "H2OTypeConverters.scalaArrayToPythonArray(value)"
    case _ => "value"
  }
}
