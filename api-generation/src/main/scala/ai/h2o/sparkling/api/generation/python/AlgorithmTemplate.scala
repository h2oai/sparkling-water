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

object AlgorithmTemplate
  extends ((AlgorithmSubstitutionContext, ParameterSubstitutionContext) => String)
  with PythonEntityTemplate
  with ParameterResolver {

  def apply(
      algorithmSubstitutionContext: AlgorithmSubstitutionContext,
      parameterSubstitutionContext: ParameterSubstitutionContext): String = {
    val parameters = resolveParameters(parameterSubstitutionContext)
    val entityName = algorithmSubstitutionContext.entityName
    val paramClasses = Seq(s"${entityName}Params", "H2OCommonParams")
    val algorithmType = algorithmSubstitutionContext.algorithmType
    val parents = paramClasses ++ Seq(algorithmType) ++ algorithmSubstitutionContext.extraInheritedEntities

    val imports = Seq("pyspark.keyword_only", "ai.h2o.sparkling.Initializer", "ai.h2o.sparkling.ml.Utils.Utils") ++
      paramClasses.map(clazz => s"ai.h2o.sparkling.ml.params.$clazz.$clazz") ++
      Seq(s"ai.h2o.sparkling.ml.algos.$algorithmType.$algorithmType") ++
      algorithmSubstitutionContext.extraInheritedEntities.map(clazz => s"ai.h2o.sparkling.ml.algos.$clazz.$clazz")

    val entitySubstitutionContext =
      EntitySubstitutionContext(algorithmSubstitutionContext.namespace, entityName, parents, imports)

    generateEntity(entitySubstitutionContext) {
      s"""    @keyword_only
         |    def __init__(self,${generateDefaultValuesFromExplicitFields(parameterSubstitutionContext.explicitFields)}
         |${generateCommonDefaultValues(parameterSubstitutionContext.defaultValuesOfCommonParameters)},
         |${generateDefaultValues(parameters, parameterSubstitutionContext.explicitDefaultValues)}):
         |        Initializer.load_sparkling_jar()
         |        super($entityName, self).__init__()
         |        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.$entityName", self.uid)
         |        self._setDefaultValuesFromJava()
         |        kwargs = Utils.getInputKwargs(self)
         |        self._set(**kwargs)""".stripMargin
    }
  }

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

  def getDefaultValue(parameter: Parameter): String = {
    parameter.defaultValue.toString
  }

  private def stringify(value: Any): String = value match {
    case a: Array[_] => s"[${a.map(stringify).mkString(", ")}]"
    case b: Boolean => b.toString.capitalize
    case s: String => s""""$s""""
    case v if v == "null" => "None"
    case v if v == null => "None"
    case v if v.getClass.isEnum => s""""$v""""
    case v => v.toString
  }
}
