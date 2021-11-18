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

import ai.h2o.sparkling.ml.algos
import ai.h2o.sparkling.ml.models.{H2OGBMMOJOModel, H2OGLMMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class RegressionMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  private lazy val Array(trainingDataset, validationDataset) = dataset.randomSplit(Array(0.8, 0.2))

  private def assertMetrics[T](model: H2OMOJOModel): Unit = {
    assertMetrics[T](model.getTrainingMetricsObject(), model.getTrainingMetrics())
    assertMetrics[T](model.getValidationMetricsObject(), model.getValidationMetrics())
    assert(model.getCrossValidationMetricsObject() == null)
    assert(model.getCrossValidationMetrics() == Map())
  }

  private def assertMetrics[T](metricsObject: H2OMetrics, metrics: Map[String, Double]): Unit = {
    metricsObject.isInstanceOf[T] should be(true)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(metricsObject, metrics)
  }

  test("test regression metric objects") {
    val algo = new algos.H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = algo.fit(dataset)
    assertMetrics[H2ORegressionMetrics](model)

    model.write.overwrite().save("ml/build/gbm_regression_model_metrics")
    val loadedModel = H2OGBMMOJOModel.load("ml/build/gbm_regression_model_metrics")
    assertMetrics[H2ORegressionMetrics](loadedModel)
  }


  test("test calculation of regression metric objects on arbitrary dataset") {
    val algo = new algos.H2OGBM()
      .setValidationDataFrame(validationDataset)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = algo.fit(trainingDataset)


    val trainingMetrics = model.getMetrics(trainingDataset)
    val trainingMetricsObject = model.getMetricsObject(trainingDataset)
    val validationMetrics = model.getMetrics(validationDataset)
    val validationMetricsObject = model.getMetricsObject(validationDataset)
    val expectedTrainingMetrics = model.getTrainingMetrics()
    val expectedValidationMetrics = model.getValidationMetrics()

    MetricsAssertions.assertEqual(expectedTrainingMetrics, trainingMetrics, tolerance = 0.0001)
    MetricsAssertions.assertEqual(expectedValidationMetrics, validationMetrics)
    val ignoredGetters = Set("getCustomMetricValue", "getScoringTime")
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(trainingMetricsObject, trainingMetrics, ignoredGetters)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(validationMetricsObject, validationMetrics, ignoredGetters)
  }

  test("test regression glm metric objects") {
    val algo = new algos.H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = algo.fit(dataset)
    assertMetrics[H2ORegressionGLMMetrics](model)

    model.write.overwrite().save("ml/build/glm_regression_model_metrics")
    val loadedModel = H2OGLMMOJOModel.load("ml/build/glm_regression_model_metrics")
    assertMetrics[H2ORegressionGLMMetrics](loadedModel)
  }

  test("test calculation of regression glm metric objects on arbitrary dataset") {
    val algo = new algos.H2OGLM()
      .setValidationDataFrame(validationDataset)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = algo.fit(trainingDataset)


    val trainingMetrics = model.getMetrics(trainingDataset)
    val trainingMetricsObject = model.getMetricsObject(trainingDataset)
    val validationMetrics = model.getMetrics(validationDataset)
    val validationMetricsObject = model.getMetricsObject(validationDataset)
    val expectedTrainingMetrics = model.getTrainingMetrics()
    val expectedValidationMetrics = model.getValidationMetrics()

    MetricsAssertions.assertEqual(expectedTrainingMetrics, trainingMetrics, tolerance = 0.00001)
    MetricsAssertions.assertEqual(expectedValidationMetrics, validationMetrics)
    val ignoredGetters = Set("getCustomMetricValue", "getScoringTime")
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(trainingMetricsObject, trainingMetrics, ignoredGetters)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(validationMetricsObject, validationMetrics, ignoredGetters)
  }

  test("test calculation of regression gam metric objects on arbitrary dataset") {
    val algo = new algos.H2OGAM()
      .setValidationDataFrame(validationDataset)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
      .setGamCols(Array(Array("PSA")))
      .setLabelCol("AGE")
    val model = algo.fit(trainingDataset)


    val trainingMetrics = model.getMetrics(trainingDataset)
    val trainingMetricsObject = model.getMetricsObject(trainingDataset)
    val validationMetrics = model.getMetrics(validationDataset)
    val validationMetricsObject = model.getMetricsObject(validationDataset)
    val expectedTrainingMetrics = model.getTrainingMetrics()
    val expectedValidationMetrics = model.getValidationMetrics()

    MetricsAssertions.assertEqual(expectedTrainingMetrics, trainingMetrics, tolerance = 0.00001)
    MetricsAssertions.assertEqual(expectedValidationMetrics, validationMetrics)
    val ignoredGetters = Set("getCustomMetricValue", "getScoringTime")
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(trainingMetricsObject, trainingMetrics, ignoredGetters)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(validationMetricsObject, validationMetrics, ignoredGetters)
  }
}
