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

package ai.h2o.sparkling.model

import ai.h2o.sparkling.backend.external.RestEncodingUtils
import com.google.gson.{JsonArray, JsonObject, JsonPrimitive}
import hex._
import org.apache.spark.h2o.H2OBaseModel
import water.util.PojoUtils

import scala.collection.JavaConverters._

/**
 * Utilities to extract information from models obtained via REST and binary models
 */
trait H2OModelExtractionUtils extends RestEncodingUtils {
  protected def extractModelCategory(model: H2OBaseModel): H2OModelCategory.Value = {
    H2OModelCategory.fromString(model._output.getModelCategory.toString)
  }

  protected def extractModelCategory(modelJson: JsonObject): H2OModelCategory.Value = {
    val json = modelJson.get("output").getAsJsonObject
    H2OModelCategory.fromString(json.get("model_category").getAsString)
  }

  protected def extractAllMetrics(model: H2OBaseModel): H2OMetricsHolder = {
    val trainingMetrics = extractMetrics(model._output._training_metrics)
    val validationMetrics = extractMetrics(model._output._validation_metrics)
    val crossValidationMetrics = extractMetrics(model._output._cross_validation_metrics)
    H2OMetricsHolder(trainingMetrics, validationMetrics, crossValidationMetrics)
  }

  protected def extractAllMetrics(modelJson: JsonObject): H2OMetricsHolder = {
    val json = modelJson.get("output").getAsJsonObject
    val trainingMetrics = extractMetrics(json, "training_metrics")
    val validationMetrics = extractMetrics(json, "validation_metrics")
    val crossValidationMetrics = extractMetrics(json, "cross_validation_metrics")
    H2OMetricsHolder(trainingMetrics, validationMetrics, crossValidationMetrics)
  }

  protected def extractParams(model: H2OBaseModel, selectedParams: Array[String]): Map[String, String] = {
    selectedParams.map { paramName =>
      val value = PojoUtils.getFieldEvenInherited(model._parms, paramName).get(model._parms)
      val stringValue = value match {
        case v: Array[_] => stringifyArray(v)
        case v => stringifyPrimitiveParam(v)
      }
      paramName -> stringValue
    }.sorted.toMap
  }

  protected def extractParams(modelJson: JsonObject): Map[String, String] = {
    val parameters = modelJson.get("parameters").getAsJsonArray.asScala.toArray
    parameters.flatMap { param =>
      val name = param.getAsJsonObject.get("name").getAsString
      val value = param.getAsJsonObject.get("actual_value")
      val stringValue = value match {
        case v: JsonPrimitive => Some(stringifyPrimitiveParam(v))
        case v: JsonArray => Some(stringifyArray(v.asScala.toArray))
        case _ =>
          // don't put more complex type to output yet
          None
      }
      stringValue.map(name -> _)
    }.sorted.toMap
  }

  private def extractMetrics(modelMetrics: ModelMetrics): Map[H2OMetric, Double] = {
    if (modelMetrics == null) {
      Map.empty
    } else {
      val specificMetrics = modelMetrics match {
        case regressionGLM: ModelMetricsRegressionGLM =>
          Seq(
            H2OMetric.MeanResidualDeviance -> regressionGLM._mean_residual_deviance,
            H2OMetric.NullDeviance -> regressionGLM._resDev,
            H2OMetric.ResidualDegreesOfFreedom -> regressionGLM._residualDegressOfFreedom.toDouble,
            H2OMetric.NullDeviance -> regressionGLM._nullDev,
            H2OMetric.NullDegreesOfFreedom -> regressionGLM._nullDegressOfFreedom.toDouble,
            H2OMetric.AIC -> regressionGLM._AIC,
            H2OMetric.R2 -> regressionGLM.r2()
          )
        case regression: ModelMetricsRegression =>
          Seq(
            H2OMetric.MeanResidualDeviance -> regression._mean_residual_deviance,
            H2OMetric.R2 -> regression.r2()
          )
        case binomialGLM: ModelMetricsBinomialGLM =>
          Seq(
            H2OMetric.AUC -> binomialGLM.auc,
            H2OMetric.Gini -> binomialGLM._auc._gini,
            H2OMetric.Logloss -> binomialGLM.logloss,
            H2OMetric.F1 -> binomialGLM.cm.f1,
            H2OMetric.F2 -> binomialGLM.cm.f2,
            H2OMetric.F0point5 -> binomialGLM.cm.f0point5,
            H2OMetric.Accuracy -> binomialGLM.cm.accuracy,
            H2OMetric.Error -> binomialGLM.cm.err,
            H2OMetric.Precision -> binomialGLM.cm.precision,
            H2OMetric.Recall -> binomialGLM.cm.recall,
            H2OMetric.MCC -> binomialGLM.cm.mcc,
            H2OMetric.MaxPerClassError -> binomialGLM.cm.max_per_class_error,
            H2OMetric.ResidualDeviance -> binomialGLM._resDev,
            H2OMetric.ResidualDegreesOfFreedom -> binomialGLM._residualDegressOfFreedom.toDouble,
            H2OMetric.NullDeviance -> binomialGLM._nullDev,
            H2OMetric.NullDegreesOfFreedom -> binomialGLM._nullDegressOfFreedom.toDouble,
            H2OMetric.AIC -> binomialGLM._AIC
          )
        case binomial: ModelMetricsBinomial =>
          Seq(
            H2OMetric.AUC -> binomial.auc,
            H2OMetric.Gini -> binomial._auc._gini,
            H2OMetric.Logloss -> binomial.logloss,
            H2OMetric.F1 -> binomial.cm.f1,
            H2OMetric.F2 -> binomial.cm.f2,
            H2OMetric.F0point5 -> binomial.cm.f0point5,
            H2OMetric.Accuracy -> binomial.cm.accuracy,
            H2OMetric.Error -> binomial.cm.err,
            H2OMetric.Precision -> binomial.cm.precision,
            H2OMetric.Recall -> binomial.cm.recall,
            H2OMetric.MCC -> binomial.cm.mcc,
            H2OMetric.MaxPerClassError.->(binomial.cm.max_per_class_error)
          )

        case multinomial: ModelMetricsMultinomial =>
          Seq(
            H2OMetric.Logloss -> multinomial.logloss,
            H2OMetric.Error -> multinomial.cm.err,
            H2OMetric.MaxPerClassError -> multinomial.cm.max_per_class_error,
            H2OMetric.Accuracy -> multinomial.cm.accuracy
          )
        case _ => Seq()
      }
      val allMetrics = specificMetrics ++ Seq(
        H2OMetric.MSE -> modelMetrics.mse,
        H2OMetric.RMSE -> modelMetrics.rmse()
      )
      allMetrics.sorted(H2OMetricOrdering).toMap
    }
  }

  private def extractMetrics(json: JsonObject, metricType: String): Map[H2OMetric, Double] = {
    if (json.get(metricType).isJsonNull) {
      Map.empty
    } else {
      val metricGroup = json.getAsJsonObject(metricType)
      val fields = metricGroup.entrySet().asScala.map(_.getKey)
      val metrics = H2OMetric.values().flatMap { metric =>
        val metricName = metric.toString
        val fieldName = fields.find(_.equalsIgnoreCase(metricName))
        if (fieldName.isDefined) {
          Some(metric -> metricGroup.get(fieldName.get).getAsDouble)
        } else {
          None
        }
      }
      metrics.sorted(H2OMetricOrdering).toMap
    }
  }

  private object H2OMetricOrdering extends Ordering[(H2OMetric, Double)] {
    def compare(a: (H2OMetric, Double), b: (H2OMetric, Double)): Int = a._1.name().compare(b._1.name())
  }
}
