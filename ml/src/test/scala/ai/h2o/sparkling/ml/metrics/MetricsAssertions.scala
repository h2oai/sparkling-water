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

package ai.h2o.sparkling.ml.metrics

import ai.h2o.sparkling.ml.models.H2OMOJOModel
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers

object MetricsAssertions extends Matchers {
  def assertMetricsObjectAgainstMetricsMap(
      metricsObject: H2OMetrics,
      metrics: Map[String, Double],
      ignoredGetters: Set[String] = Set("getCustomMetricValue")): Unit = {
    for (getter <- metricsObject.getClass.getMethods
         if getter.getName.startsWith("get")
         if !ignoredGetters.contains("getCustomMetricValue")
         if getter.getParameterCount == 0
         if getter.getReturnType.isPrimitive) {
      val value = getter.invoke(metricsObject)
      val metricName = getter.getName.substring(3)
      assert(metrics.contains(metricName), s"$metricName is not defined in H2OMetrics.")
      val metricValue = metrics.get(metricName).get
      if (metricValue.isNaN) {
        assert(value.asInstanceOf[Double].isNaN)
      } else {
        value shouldEqual metricValue
      }
    }
  }

  def assertEqual(
      expected: Map[String, Double],
      actual: Map[String, Double],
      ignored: Set[String] = Set("ScoringTime"),
      tolerance: Double = 0.0): Unit = {
    val expectedKeys = expected.keySet
    val actualKeys = actual.keySet

    expectedKeys shouldEqual actualKeys

    for (key <- expectedKeys.diff(ignored)) {
      if (expected(key).isNaN && actual(key).isNaN) {
        // Values are equal
      } else if (tolerance > 0.0) {
        expected(key) shouldBe (actual(key) +- tolerance)
      } else {
        expected(key) shouldBe actual(key)
      }
    }
  }

  def assertEssentialMetrics(
      model: H2OMOJOModel,
      trainingDataset: DataFrame,
      validationDataset: DataFrame,
      trainingMetricsTolerance: Double = 0.0,
      validationMetricsTolerance: Double = 0.0): Unit = {
    val trainingMetrics = model.getMetrics(trainingDataset)
    val trainingMetricsObject = model.getMetricsObject(trainingDataset)
    val validationMetrics = model.getMetrics(validationDataset)
    val validationMetricsObject = model.getMetricsObject(validationDataset)
    val expectedTrainingMetrics = model.getTrainingMetrics()
    val expectedValidationMetrics = model.getValidationMetrics()

    MetricsAssertions.assertEqual(expectedTrainingMetrics, trainingMetrics, tolerance = trainingMetricsTolerance)
    MetricsAssertions.assertEqual(expectedValidationMetrics, validationMetrics, tolerance = validationMetricsTolerance)
    val ignoredGetters = Set("getCustomMetricValue", "getScoringTime")
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(trainingMetricsObject, trainingMetrics, ignoredGetters)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(validationMetricsObject, validationMetrics, ignoredGetters)
  }
}
