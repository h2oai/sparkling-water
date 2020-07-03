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
import ai.h2o.sparkling.api.generation.python.AlgorithmTemplate.resolveParameters

object ProblemSpecificAlgorithmTemplate
  extends ((String, ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext]) => String)
  with ScalaEntityTemplate {

  def apply(
      problemType: String,
      algorithmSubstitutionContext: ProblemSpecificAlgorithmSubstitutionContext,
      parameterSubstitutionContexts: Seq[ParameterSubstitutionContext]): String = {
    val parameterNames = parameterSubstitutionContexts.flatMap(resolveParameters).map(_.h2oName)
    val parentEntityName = algorithmSubstitutionContext.parentEntityName
    val parentNamespace = algorithmSubstitutionContext.parentNamespace
    val entityName = algorithmSubstitutionContext.entityName
    val namespace = algorithmSubstitutionContext.namespace
    val problemTypeClass = "H2O" + entityName.substring(parentEntityName.length)

    val checks = algorithmSubstitutionContext.parametersToCheck.map { parameter =>
      s"${parameter.capitalize}For${problemType.capitalize}Check"
    }
    val parents = Seq(parentEntityName, problemTypeClass) ++ checks

    val imports = Seq(
      s"${parentNamespace}.${parentEntityName}",
      "ai.h2o.sparkling.ml.utils.H2OParamsReadable",
      "org.apache.spark.ml.util.Identifiable")

    val classParameters = "(override val uid: String)"

    val entitySubstitutionContext =
      EntitySubstitutionContext(namespace, entityName, parents, imports, classParameters)

    val overriddenDefaultValues = algorithmSubstitutionContext.overriddenDefaultValues
    val defaultValues = overriddenDefaultValues
      .filter((keyValuePair) => parameterNames.contains(keyValuePair._1))
      .map(value => s"${value._1} -> ${stringify(value._2)}")
    val defaultValueSection = if (defaultValues.isEmpty) "" else s"\n\n  set(${defaultValues.mkString(",\n")})"

    val algorithmClass = generateEntity(entitySubstitutionContext, "class") {
      s"  def this() = this(Identifiable.randomUID(classOf[$entityName].getSimpleName))$defaultValueSection"
    }

    val algorithmObject = s"object $entityName extends H2OParamsReadable[$entityName]"

    s"""$algorithmClass
       |$algorithmObject
     """.stripMargin
  }
}
