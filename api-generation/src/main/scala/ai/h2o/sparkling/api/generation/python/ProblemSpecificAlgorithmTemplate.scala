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

object ProblemSpecificAlgorithmTemplate
  extends ((String, ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext]) => String)
  with AlgorithmTemplateBase
  with ParameterResolver {

  def apply(
      problemType: String,
      algorithmSubstitutionContext: ProblemSpecificAlgorithmSubstitutionContext,
      parameterSubstitutionContexts: Seq[ParameterSubstitutionContext]): String = {
    val parameters = parameterSubstitutionContexts.flatMap(resolveParameters)
    val commonSubstitutionContext = parameterSubstitutionContexts.head
    val entityName = algorithmSubstitutionContext.entityName
    val parentEntityName = algorithmSubstitutionContext.parentEntityName
    val parentNamespace = algorithmSubstitutionContext.parentNamespace
    val parents = Seq(parentEntityName)

    val imports = Seq(
      "pyspark.keyword_only",
      "ai.h2o.sparkling.Initializer",
      "ai.h2o.sparkling.ml.Utils.Utils",
      s"$parentNamespace.$parentEntityName.$parentEntityName")

    val entitySubstitutionContext =
      EntitySubstitutionContext(algorithmSubstitutionContext.namespace, entityName, parents, imports)

    val overriddenDefaultValues = algorithmSubstitutionContext.overriddenDefaultValues
    val explicitFields = commonSubstitutionContext.explicitFields.map { explicitField =>
      overriddenDefaultValues.get(explicitField.name) match {
        case None => explicitField
        case Some(value) => explicitField.copy(defaultValue = value)
      }
    }
    val commonDefaultValues = commonSubstitutionContext.defaultValuesOfCommonParameters ++
      overriddenDefaultValues.keySet
        .intersect(commonSubstitutionContext.defaultValuesOfCommonParameters.keySet)
        .map(k => k -> overriddenDefaultValues(k))
    val explicitDefaultValues = commonSubstitutionContext.explicitDefaultValues ++ overriddenDefaultValues

    generateEntity(entitySubstitutionContext) {
      s"""    @keyword_only
         |    def __init__(self,${generateDefaultValuesFromExplicitFields(explicitFields)}
         |${generateCommonDefaultValues(commonDefaultValues)},
         |${generateDefaultValues(parameters, explicitDefaultValues)}):
         |        Initializer.load_sparkling_jar()
         |        super($entityName, self).__init__()
         |        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.$entityName", self.uid)
         |        self._setDefaultValuesFromJava()
         |        kwargs = Utils.getInputKwargs(self)
         |        self._set(**kwargs)
         |        self._transfer_params_to_java()""".stripMargin
    }
  }
}
