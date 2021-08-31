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

import ai.h2o.sparkling.api.generation.common.{EntitySubstitutionContext, Metric, MetricResolver, ModelMetricsSubstitutionContext}

object ModelMetricsTemplate
  extends (ModelMetricsSubstitutionContext => String)
  with MetricResolver
  with REntityTemplate {

  def apply(substitutionContext: ModelMetricsSubstitutionContext): String = {
    val metrics = resolveMetrics(substitutionContext)

    val imports = substitutionContext.parentEntities.diff(Seq("H2OGLMMetrics"))
    val parentEntities = imports.map(p => s"rsparkling.$p") // Renaming due to collision with H2O R API

    val entitySubstitutionContext = EntitySubstitutionContext(
      substitutionContext.namespace,
      "rsparkling." + substitutionContext.entityName, // Renaming due to collision with H2O R API
      parentEntities,
      imports)

    generateEntity(entitySubstitutionContext) {
      generateGetterMethods(metrics)
    }
  }

  private def generateGetterMethods(metrics: Seq[Metric]): String = {
    metrics
      .map { metric =>
        val methodName = s"get${metric.swMetricName}"
        val valueExtraction = s"""invoke(.self$$javaObject, "$methodName")"""
        val valueConversion = generateValueConversion(metric, valueExtraction)
        s"""    $methodName = function() { $valueConversion }"""
      }
      .mkString(",\n")
  }

  private def generateValueConversion(metric: Metric, value: String): String = metric.dataType.getSimpleName match {
    case "TwoDimTableV3" | "ConfusionMatrixV3" => s"sdf_register($value)"
    case _ => value
  }
}
