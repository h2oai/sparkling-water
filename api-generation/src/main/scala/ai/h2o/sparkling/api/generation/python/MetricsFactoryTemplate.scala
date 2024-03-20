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

import ai.h2o.sparkling.api.generation.common.{EntitySubstitutionContext, ModelMetricsSubstitutionContext}

object MetricsFactoryTemplate extends ((Seq[ModelMetricsSubstitutionContext]) => String) with PythonEntityTemplate {

  def apply(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    val metricClasses = getEntityNames(metricSubstitutionContexts)
    val imports = Seq("py4j.java_gateway.JavaObject") ++
      metricClasses.map(metricClass => s"ai.h2o.sparkling.ml.metrics.$metricClass.$metricClass")

    val entitySubstitutionContext = EntitySubstitutionContext(
      metricSubstitutionContexts.head.namespace,
      "H2OMetricsFactory",
      inheritedEntities = Seq.empty,
      imports)

    generateEntity(entitySubstitutionContext) {
      s"""    def __init__(self, javaObject):
         |        self._java_obj = javaObject
         |
         |    @staticmethod
         |    def fromJavaObject(javaObject):
         |        if javaObject is None:
         |            return None
         |${generatePatternMatchingCases(metricSubstitutionContexts)}
         |        else:
         |            return H2OCommonMetrics(javaObject)""".stripMargin
    }
  }

  private def getEntityNames(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): Seq[String] = {
    metricSubstitutionContexts
      .map { metricSubstitutionContext =>
        if (metricSubstitutionContext.entityName.endsWith("Base")) {
          metricSubstitutionContext.entityName.substring(0, metricSubstitutionContext.entityName.length - 4)
        } else {
          metricSubstitutionContext.entityName
        }
      }
  }

  private def generatePatternMatchingCases(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    getEntityNames(metricSubstitutionContexts)
      .map { entityName =>
        s"""        elif javaObject.getClass().getSimpleName() == "$entityName":
           |            return $entityName(javaObject)""".stripMargin
      }
      .mkString("\n")
  }
}
