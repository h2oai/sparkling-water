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

import java.util

import hex.grid.HyperSpaceSearchCriteria
import hex.grid.HyperSpaceSearchCriteria.{CartesianSearchCriteria, RandomDiscreteValueSearchCriteria}
import hex.schemas.HyperSpaceSearchCriteriaV99
import hex.schemas.HyperSpaceSearchCriteriaV99.{CartesianSearchCriteriaV99, RandomDiscreteValueSearchCriteriaV99}

trait GridSearchConfiguration {
  def gridSearchParameterConfiguration: Seq[ParameterSubstitutionContext] = {
    class DummySearchCriteria extends HyperSpaceSearchCriteriaV99[HyperSpaceSearchCriteria, DummySearchCriteria]

    val gridSearchParameters = Seq[(String, Class[_], Class[_], Seq[String])](
      (
        "H2OGridSearchRandomDiscreteCriteriaParams",
        classOf[RandomDiscreteValueSearchCriteriaV99],
        classOf[RandomDiscreteValueSearchCriteria],
        Seq("strategy")),
      (
        "H2OGridSearchCartesianCriteriaParams",
        classOf[CartesianSearchCriteriaV99],
        classOf[CartesianSearchCriteria],
        Seq("strategy")),
      ("H2OGridSearchCommonCriteriaParams", classOf[DummySearchCriteria], classOf[CartesianSearchCriteria], Seq.empty))

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], extraIgnoredParameters) <- gridSearchParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        ignoredParameters = Seq("__meta") ++ extraIgnoredParameters,
        explicitFields = Seq.empty,
        deprecatedFields = Seq.empty,
        explicitDefaultValues = Map.empty,
        typeExceptions = Map.empty,
        defaultValueSource = DefaultValueSource.Getter,
        defaultValuesOfCommonParameters = Map(
          "algo" -> null,
          "hyperParameters" -> new util.HashMap[String, AnyRef](),
          "selectBestModelBy" -> "AUTO",
          "parallelism" -> 1),
        generateParamTag = false)
  }

  def gridSearchAlgorithmContext: AlgorithmSubstitutionContext = {
    AlgorithmSubstitutionContext(
      namespace = "ai.h2o.sparkling.ml.algos",
      "H2OGridSearch",
      null,
      "H2OAlgorithm",
      Seq("H2OGridSearchExtras"),
      false)
  }
}
