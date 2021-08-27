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

object ModelMetricsTemplate
  extends (ModelMetricsSubstitutionContext => String)
  with PythonEntityTemplate
  with MetricResolver {

  def apply(substitutionContext: ModelMetricsSubstitutionContext): String = {
    val metrics = resolveMetrics(substitutionContext)

    val parentEntities = substitutionContext.parentEntities.diff(Seq("H2OMetrics", "H2OGLMMetrics"))

    val imports = Seq("pyspark.ml.param.*", "ai.h2o.sparkling.ml.params.H2OTypeConverters.H2OTypeConverters") ++
      parentEntities.map(parent => s"ai.h2o.sparkling.ml.metrics.$parent.$parent")

    val entitySubstitutionContext =
      EntitySubstitutionContext(substitutionContext.namespace, substitutionContext.entityName, parentEntities, imports)

    generateEntity(entitySubstitutionContext) {
      s"""    def __init__(self, java_obj):
        |        self._java_obj = java_obj
        |
        |${generateGetterMethods(metrics)}""".stripMargin
    }
  }

  private def generateGetterMethods(metrics: Seq[Metric]): String = {
    metrics
      .map { metric =>
        val valueConversion = generateValueConversion(metric)
        s"""    def get${metric.swMetricName}(self):
           |        \"\"\"
           |        ${resolveComment(metric)}
           |        \"\"\"
           |        value = self._java_obj.get${metric.swMetricName}()
           |        return $valueConversion""".stripMargin
      }
      .mkString("\n\n")
  }

  private def generateValueConversion(metric: Metric): String = metric.dataType match {
    case x if x.isPrimitive => "value"
    case x if x.getSimpleName == "String" => "value"
    case x if x.getSimpleName == "TwoDimTableV3" => "H2OTypeConverters.scalaToPythonDataFrame(value)"
    case x if x.getSimpleName == "ConfusionMatrixV3" => "H2OTypeConverters.scalaToPythonDataFrame(value)"
  }

  private def resolveComment(metric: Metric): String = {
    if (metric.comment.endsWith(".")) metric.comment else metric.comment + "."
  }
}
