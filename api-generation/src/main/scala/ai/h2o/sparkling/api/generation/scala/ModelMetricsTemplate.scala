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

object ModelMetricsTemplate
  extends (ModelMetricsSubstitutionContext => String)
  with ScalaEntityTemplate
  with MetricResolver {

  def apply(substitutionContext: ModelMetricsSubstitutionContext): String = {
    val metrics = resolveMetrics(substitutionContext)

    val imports = Seq(
      "com.google.gson.JsonObject",
      "org.apache.spark.sql.DataFrame",
      "org.apache.spark.ml.param.ParamMap",
      "org.apache.spark.ml.util.Identifiable",
      "ai.h2o.sparkling.utils.DataFrameSerializationWrappers._")
    val parameters = "(override val uid: String)"

    val annotations = Seq(s"""@MetricsDescription(description = "${substitutionContext.classDescription}")""")

    val entitySubstitutionContext = EntitySubstitutionContext(
      substitutionContext.namespace,
      substitutionContext.entityName,
      substitutionContext.parentEntities,
      imports,
      parameters,
      annotations)

    generateEntity(entitySubstitutionContext, "class") {
      s"""def this() = this(Identifiable.randomUID("${substitutionContext.entityName}"))
         |
         |${generateParameterDefinitions(metrics)}
         |${generateDefaults(metrics)}
         |
         |  /// Getters
         |${generateGetters(metrics)}
         |
         |  override def setMetrics(json: JsonObject, context: String): Unit = {
         |    super.setMetrics(json, context)
         |
         |${generateValueAssignments(metrics)}
         |  }
         |
         |  override def copy(extra: ParamMap): this.type = defaultCopy(extra)""".stripMargin
    }
  }

  private def resolveComment(metric: Metric): String = {
    if (metric.comment.endsWith(".")) metric.comment else metric.comment + "."
  }

  private def resolveMetricType(metric: Metric): String = metric.dataType match {
    case x if x.isPrimitive => x.getSimpleName.capitalize
    case x if x.getSimpleName == "String" => "String"
    case x if x.getSimpleName == "TwoDimTableV3" => "DataFrame"
    case x if x.getSimpleName == "ConfusionMatrixV3" => "DataFrame"
  }

  private def generateDefaults(metrics: Seq[Metric]): String = {
    metrics
//      .filter(!_.overridden)
      .flatMap { metric =>
        if (metric.dataType.getSimpleName == "double")
          Some(s"\n  setDefault(${metric.swFieldName} -> Double.NaN)")
        else if (metric.dataType.getSimpleName == "float")
          Some(s"\n  setDefault(${metric.swFieldName} -> Float.NaN)")
        else if (!metric.dataType.isPrimitive)
          Some(s"\n  setDefault(${metric.swFieldName} -> null)")
        else
          None
      }
      .mkString("")
  }

  private def generateGetters(metrics: Seq[Metric]): String = {
    metrics
      .map { metric =>
        val metricType = resolveMetricType(metric)
        val overridden = if (metric.overridden) "override" else ""
        s"""  /**
         |    * ${resolveComment(metric)}
         |    */
         |  ${overridden} def get${metric.swMetricName}(): $metricType = $$(${metric.swFieldName})""".stripMargin
      }
      .mkString("\n\n")
  }

  private def generateParameterDefinitions(metrics: Seq[Metric]): String = {
    metrics
      .map { metric =>
        val metricType = resolveMetricType(metric)
        val prefix = if (metric.dataType.isPrimitive) {
          metric.dataType.getSimpleName
        } else {
          s"nullable${metricType.capitalize}"
        }
        val constructorMethod = prefix + "Param"
        val comment = resolveComment(metric)
        val overridden = if (metric.overridden) "override " else ""
        s"""  protected ${overridden}val ${metric.swFieldName} = ${constructorMethod}(
         |    name = "${metric.swFieldName}",
         |    doc = \"\"\"$comment\"\"\")""".stripMargin
      }
      .mkString("\n\n")
  }

  private def generateValueExtraction(metric: Metric): String = metric.dataType match {
    case x if x.isPrimitive => s"""json.get("${metric.h2oName}").getAs${x.getSimpleName.capitalize}()"""
    case x if x.getSimpleName == "String" =>
      s"""if (json.get("${metric.h2oName}").isJsonNull) null else json.get("${metric.h2oName}").getAsString()"""
    case x if x.getSimpleName == "TwoDimTableV3" => s"""toWrapper(jsonFieldToDataFrame(json, "${metric.h2oName}"))"""
    case x if x.getSimpleName == "ConfusionMatrixV3" =>
      s"""toWrapper(nestedJsonFieldToDataFrame(json, "${metric.h2oName}", "table"))"""
  }

  private def generateValueAssignments(metrics: Seq[Metric]): String = {
    metrics
      .map { metric =>
        val parsing =
          s"""    if (json.has("${metric.h2oName}")) {
             |      try {
             |        set("${metric.swFieldName}", ${generateValueExtraction(metric)})
             |      } catch {
             |        case e: Throwable if System.getProperty("spark.testing", "false") != "true" =>
             |          logError("Unsuccessful try to extract '${metric.h2oName}' from " + context, e)
             |      }
             |    }"""
        val mandatoryCheck = if (MetricFieldExceptions.optional().contains(metric.h2oName)) {
          ""
        } else {
          s""" else {
             |      val message = "The metric '${metric.h2oName}' in " + context + " does not exist."
             |      if (System.getProperty("spark.testing", "false") != "true") {
             |        logWarning(message)
             |      } else {
             |        throw new AssertionError(message)
             |      }
             |    }""".stripMargin
        }
        parsing + mandatoryCheck
      }
      .mkString("\n\n")
  }
}
