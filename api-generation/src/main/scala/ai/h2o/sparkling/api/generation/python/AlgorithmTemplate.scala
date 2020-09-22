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
  extends ((AlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext]) => String)
  with AlgorithmTemplateBase
  with ParameterResolver {

  def apply(
      algorithmSubstitutionContext: AlgorithmSubstitutionContext,
      parameterSubstitutionContexts: Seq[ParameterSubstitutionContext]): String = {
    val parameters = parameterSubstitutionContexts.flatMap(resolveParameters)
    val commonSubstitutionContext = parameterSubstitutionContexts.head
    val entityName = algorithmSubstitutionContext.entityName
    val namespace = algorithmSubstitutionContext.namespace
    val paramClasses = Seq(s"${entityName}Params", "H2OCommonParams")
    val algorithmType = algorithmSubstitutionContext.algorithmType
    val parents = paramClasses ++ Seq(algorithmType) ++ algorithmSubstitutionContext.extraInheritedEntities
    val mojoClassName = s"${entityName}MOJOModel"
    val constructorMethods = algorithmSubstitutionContext.constructorMethods

    val imports = Seq(
      "warnings.warn",
      "pyspark.keyword_only",
      "ai.h2o.sparkling.Initializer",
      "ai.h2o.sparkling.ml.Utils.Utils",
      s"ai.h2o.sparkling.ml.algos.$algorithmType.$algorithmType") ++
      (if (constructorMethods) Seq(s"ai.h2o.sparkling.ml.models.$mojoClassName.$mojoClassName") else Seq.empty) ++
      paramClasses.map(clazz => s"ai.h2o.sparkling.ml.params.$clazz.$clazz") ++
      algorithmSubstitutionContext.extraInheritedEntities.map(clazz => s"ai.h2o.sparkling.ml.algos.$clazz.$clazz")

    val entitySubstitutionContext = EntitySubstitutionContext(namespace, entityName, parents, imports)

    val additionalMethod = if (constructorMethods) {
      s"""
         |    def _create_model(self, javaModel):
         |        return ${entityName}MOJOModel(javaModel)
         |""".stripMargin
    } else {
      ""
    }

    val clazz =
      generateAlgorithmClass(
        entityName,
        entityName,
        namespace,
        parameters,
        entitySubstitutionContext,
        commonSubstitutionContext)

    clazz + additionalMethod
  }
}
