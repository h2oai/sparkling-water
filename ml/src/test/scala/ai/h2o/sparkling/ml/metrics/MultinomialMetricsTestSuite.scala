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

import ai.h2o.sparkling.ml.algos._
import ai.h2o.sparkling.ml.models.{H2OGBMMOJOModel, H2OGLMMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class MultinomialMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))
    .repartition(20)

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
    val multinomialObject = metricsObject.asInstanceOf[H2OMultinomialMetrics]
    multinomialObject.getConfusionMatrix().count() > 0
    multinomialObject.getConfusionMatrix().columns.length > 0
    multinomialObject.getHitRatioTable().count() > 0
    multinomialObject.getHitRatioTable().columns.length > 0
  }

  private def assertMetrics(
      model: H2OMOJOModel,
      trainingDataset: DataFrame,
      validationDataset: DataFrame,
      trainingMetricsTolerance: Double = 0.0,
      validationMetricsTolerance: Double = 0.0,
      skipExtraMetrics:Boolean = false): Unit = {
    MetricsAssertions.assertEssentialMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance,
      validationMetricsTolerance,
      skipExtraMetrics)

    val trainingMetricObject = model.getMetricsObject(trainingDataset).asInstanceOf[H2OMultinomialMetrics]
    val expectedTrainingMetricObject = model.getTrainingMetricsObject().asInstanceOf[H2OMultinomialMetrics]
    TestUtils.assertDataFramesAreEqual(
      trainingMetricObject.getMultinomialAUCTable(),
      expectedTrainingMetricObject.getMultinomialAUCTable(),
      "Type",
      trainingMetricsTolerance)
    TestUtils.assertDataFramesAreEqual(
      trainingMetricObject.getMultinomialPRAUCTable(),
      expectedTrainingMetricObject.getMultinomialPRAUCTable(),
      "Type",
      trainingMetricsTolerance)
    TestUtils.assertDataFramesAreIdentical(
      trainingMetricObject.getConfusionMatrix(),
      expectedTrainingMetricObject.getConfusionMatrix())
    TestUtils.assertDataFramesAreEqual(
      trainingMetricObject.getHitRatioTable(),
      expectedTrainingMetricObject.getHitRatioTable(),
      "K",
      trainingMetricsTolerance)

    val validationMetricObject = model.getMetricsObject(validationDataset).asInstanceOf[H2OMultinomialMetrics]
    val expectedValidationMetricObject = model.getValidationMetricsObject().asInstanceOf[H2OMultinomialMetrics]
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getMultinomialAUCTable(),
      expectedValidationMetricObject.getMultinomialAUCTable(),
      "Type",
      validationMetricsTolerance)
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getMultinomialPRAUCTable(),
      expectedValidationMetricObject.getMultinomialPRAUCTable(),
      "Type",
      validationMetricsTolerance)
    TestUtils.assertDataFramesAreIdentical(
      validationMetricObject.getConfusionMatrix(),
      expectedValidationMetricObject.getConfusionMatrix())
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getHitRatioTable(),
      expectedValidationMetricObject.getHitRatioTable(),
      "K",
      validationMetricsTolerance)
  }

  test("test multinomial metric objects") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")
    val model = algo.fit(dataset)
    assertMetrics[H2OMultinomialMetrics](model)

    model.write.overwrite().save("ml/build/gbm_multinomial_model_metrics")
    val loadedModel = H2OGBMMOJOModel.load("ml/build/gbm_multinomial_model_metrics")
    assertMetrics[H2OMultinomialMetrics](loadedModel)
  }

  test("test multinomial glm metric objects") {
    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")
    val model = algo.fit(dataset)
    assertMetrics[H2OMultinomialGLMMetrics](model)

    model.write.overwrite().save("ml/build/glm_multinomial_model_metrics")
    val loadedModel = H2OGLMMOJOModel.load("ml/build/glm_multinomial_model_metrics")
    assertMetrics[H2OMultinomialGLMMetrics](loadedModel)
  }

  {
    val algorithmsAndTolerances: Seq[(H2OSupervisedAlgorithm[_], Double, Double)] = Seq(
      (new H2ODeepLearning(), 0.00001, 0.00000001),
      (new H2OXGBoost(), 0.00001, 0.00000001),
      (new H2OGBM(), 0.00001, 0.00000001),
      (new H2OGLM(), 0.00001, 0.00000001))

    for ((algorithm, trainingMetricsTolerance, validationMetricsTolerance) <- algorithmsAndTolerances) {
      val algorithmName = algorithm.getClass.getSimpleName

      test(s"test calculation of multinomial $algorithmName metrics on arbitrary dataset") {
        val algo = new H2OGBM()
          .setValidationDataFrame(validationDataset)
          .setSeed(1)
          .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
          .setColumnsToCategorical("class")
          .setAucType("MACRO_OVR")
          .setLabelCol("class")
        val model = algo.fit(trainingDataset)

        assertMetrics(
          model,
          trainingDataset,
          validationDataset,
          trainingMetricsTolerance,
          validationMetricsTolerance)
      }
    }
  }

  test("test calculation of multinomial H2ODRF metrics on arbitrary dataset") {
    val algo = new H2ODRF()
      .setValidationDataFrame(validationDataset)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setAucType("MACRO_OVR")
      .setLabelCol("class")
    val model = algo.fit(trainingDataset)

    val validationMetricsTolerance = 0.00000001
    MetricsAssertions.assertEssentialMetrics(
      model,
      trainingDataset,
      validationDataset,
      Double.PositiveInfinity,
      validationMetricsTolerance)

    val validationMetricObject = model.getMetricsObject(validationDataset).asInstanceOf[H2OMultinomialMetrics]
    val expectedValidationMetricObject = model.getValidationMetricsObject().asInstanceOf[H2OMultinomialMetrics]
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getMultinomialAUCTable(),
      expectedValidationMetricObject.getMultinomialAUCTable(),
      "Type",
      validationMetricsTolerance)
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getMultinomialPRAUCTable(),
      expectedValidationMetricObject.getMultinomialPRAUCTable(),
      "Type",
      validationMetricsTolerance)
    TestUtils.assertDataFramesAreIdentical(
      validationMetricObject.getConfusionMatrix(),
      expectedValidationMetricObject.getConfusionMatrix())
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getHitRatioTable(),
      expectedValidationMetricObject.getHitRatioTable(),
      "K",
      validationMetricsTolerance)
  }

  test("test calculation of multinomial H2ORuleFit metrics on arbitrary dataset") {
    val algo = new H2ORuleFit()
      .setValidationDataFrame(validationDataset)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setAucType("MACRO_OVR")
      .setLabelCol("class")
    val model = algo.fit(trainingDataset)

    // H2O runtime calculates metrics with GLM additions, but SW calculates just generic binomial metrics
    assertMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance = 0.00001,
      validationMetricsTolerance = 0.00000001,
      skipExtraMetrics = true)
  }

  test("test calculation of multinomial H2OGAM metrics on arbitrary dataset") {
    // Significant differences are made when number of partitions is a higher number
    val gamTrainingDataset = trainingDataset.repartition(1)
    val gamValidationDataset = validationDataset.repartition(1)
    val algo = new H2OGAM()
      .setValidationDataFrame(gamValidationDataset)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len")
      .setGamCols(Array("petal_len"))
      .setColumnsToCategorical("class")
      .setAucType("MACRO_OVR")
      .setLabelCol("class")
    val model = algo.fit(gamTrainingDataset)

    assertMetrics(
      model,
      gamTrainingDataset,
      gamValidationDataset,
      trainingMetricsTolerance = 0.0001,
      validationMetricsTolerance = 0.00000001)
  }
}
