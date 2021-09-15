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

package ai.h2o.sparkling.api.generation.r

import ai.h2o.sparkling.api.generation.common.ModelMetricsSubstitutionContext

object MetricsFactoryTemplate extends ((Seq[ModelMetricsSubstitutionContext]) => String) {

  def apply(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    val metricClasses = metricSubstitutionContexts.map(_.entityName)
    val imports = metricClasses.map(metricClass => s"""source(file.path("R", "${metricClass}.R"))""").mkString("\n")

    s"""#
       |# Licensed to the Apache Software Foundation (ASF) under one or more
       |# contributor license agreements.  See the NOTICE file distributed with
       |# this work for additional information regarding copyright ownership.
       |# The ASF licenses this file to You under the Apache License, Version 2.0
       |# (the "License"); you may not use this file except in compliance with
       |# the License.  You may obtain a copy of the License at
       |#
       |#    http://www.apache.org/licenses/LICENSE-2.0
       |#
       |# Unless required by applicable law or agreed to in writing, software
       |# distributed under the License is distributed on an "AS IS" BASIS,
       |# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       |# See the License for the specific language governing permissions and
       |# limitations under the License.
       |#
       |
       |$imports
       |
       |H2OMetricsFactory.fromJavaObject <- function(javaObject) {
       |  if (is.null(javaObject)) {
       |    NULL
       |${generateCases(metricSubstitutionContexts)}
       |  } else {
       |    rsparkling.H2OCommonMetrics(javaObject)
       |  }
       |}
       |""".stripMargin
  }

  private def generateCases(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    metricSubstitutionContexts
      .map { metricSubstitutionContext =>
        val metricsObjectName = metricSubstitutionContext.entityName
        s"""  } else if (invoke(invoke(javaObject, "getClass"), "getSimpleName") == "$metricsObjectName") {
           |    rsparkling.$metricsObjectName(javaObject)""".stripMargin
      }
      .mkString("\n")
  }
}
