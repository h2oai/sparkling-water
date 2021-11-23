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
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class BinomialMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("CAPSULE", 'CAPSULE.cast(StringType))
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
    val binomialObject = metricsObject.asInstanceOf[H2OBinomialMetrics]
    binomialObject.getConfusionMatrix().count() > 0
    binomialObject.getConfusionMatrix().columns.length > 0
    binomialObject.getGainsLiftTable().count() > 0
    binomialObject.getGainsLiftTable().columns.length > 0
    binomialObject.getMaxCriteriaAndMetricScores().count() > 0
    binomialObject.getMaxCriteriaAndMetricScores().columns.length > 0
    binomialObject.getThresholdsAndMetricScores().count() > 0
    binomialObject.getThresholdsAndMetricScores().columns.length > 0
  }

  private def assertMetrics(
      model: H2OMOJOModel,
      trainingDataset: DataFrame,
      validationDataset: DataFrame,
      trainingMetricsTolerance: Double = 0.0,
      validationMetricsTolerance: Double = 0.0,
      skipExtraMetrics: Boolean = false): Unit = {
    MetricsAssertions.assertEssentialMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance,
      validationMetricsTolerance,
      skipExtraMetrics)

    val trainingMetricObject = model.getMetricsObject(trainingDataset).asInstanceOf[H2OBinomialMetrics]
    val expectedTrainingMetricObject = model.getTrainingMetricsObject().asInstanceOf[H2OBinomialMetrics]

    TestUtils.assertDataFramesAreIdentical(
      trainingMetricObject.getConfusionMatrix(),
      expectedTrainingMetricObject.getConfusionMatrix())
    TestUtils.assertDataFramesAreEqual(
      trainingMetricObject.getThresholdsAndMetricScores(),
      expectedTrainingMetricObject.getThresholdsAndMetricScores(),
      "idx",
      trainingMetricsTolerance)
    TestUtils.assertDataFramesAreEqual(
      trainingMetricObject.getMaxCriteriaAndMetricScores(),
      expectedTrainingMetricObject.getMaxCriteriaAndMetricScores(),
      "Metric",
      trainingMetricsTolerance)
    trainingMetricObject.getGainsLiftTable() shouldBe (null) // Gains-lift table is not supported yet.

    val validationMetricObject = model.getMetricsObject(validationDataset).asInstanceOf[H2OBinomialMetrics]
    val expectedValidationMetricObject = model.getValidationMetricsObject().asInstanceOf[H2OBinomialMetrics]

    TestUtils.assertDataFramesAreIdentical(
      validationMetricObject.getConfusionMatrix(),
      expectedValidationMetricObject.getConfusionMatrix())
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getThresholdsAndMetricScores(),
      expectedValidationMetricObject.getThresholdsAndMetricScores(),
      "idx",
      validationMetricsTolerance)
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getMaxCriteriaAndMetricScores(),
      expectedValidationMetricObject.getMaxCriteriaAndMetricScores(),
      "Metric",
      validationMetricsTolerance)
    validationMetricObject.getGainsLiftTable() shouldBe (null) // Gains-lift table is not supported yet.
  }

  test("test binomial metric objects") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")

    val model = algo.fit(dataset)
    assertMetrics[H2OBinomialMetrics](model)

    model.write.overwrite().save("ml/build/gbm_binomial_model_metrics")
    val loadedModel = H2OGBMMOJOModel.load("ml/build/gbm_binomial_model_metrics")
    assertMetrics[H2OBinomialMetrics](loadedModel)
  }

  test("test binomial glm metric objects") {
    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")

    val model = algo.fit(dataset)
    assertMetrics[H2OBinomialGLMMetrics](model)

    model.write.overwrite().save("ml/build/glm_binomial_model_metrics")
    val loadedModel = H2OGLMMOJOModel.load("ml/build/glm_binomial_model_metrics")
    assertMetrics[H2OBinomialGLMMetrics](loadedModel)
  }

  {
    val algorithmsAndTolerances: Seq[(H2OSupervisedAlgorithm[_], Double, Double)] = Seq(
      (new H2ODeepLearning(), 0.00001, 0.00000001),
      (new H2OXGBoost(), 0.00005, 0.00000001),
      (new H2OGBM(), 0.00001, 0.00000001),
      (new H2OGLM(), 0.00001, 0.00000001))

    for ((algorithm, trainingMetricsTolerance, validationMetricsTolerance) <- algorithmsAndTolerances) {
      val algorithmName = algorithm.getClass.getSimpleName

      test(s"test calculation of binomial $algorithmName metric on arbitrary dataset") {
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
          .setLabelCol("CAPSULE")
        val model = algorithm.fit(trainingDataset)

        assertMetrics(model, trainingDataset, validationDataset, trainingMetricsTolerance, validationMetricsTolerance)
      }
    }
  }

  test(s"test calculation of binomial H2ORuleFit metric on arbitrary dataset") {
    val algorithm = new H2ORuleFit()
    algorithm
      .setValidationDataFrame(validationDataset)
      .setSeed(1L)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
    val model = algorithm.fit(trainingDataset)

    // H2O runtime caclulates metrics with GLM additions, but SW calculates just generic binomial metrics
    assertMetrics(model, trainingDataset, validationDataset, 0.00001, 0.00000001, skipExtraMetrics = true)
  }

  test(s"test calculation of binomial validation H2ODRF metric on arbitrary dataset") {
    val algorithm = new H2ODRF()
    algorithm
      .setValidationDataFrame(validationDataset)
      .set(algorithm.getParam("seed"), 1L)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
    val model = algorithm.fit(trainingDataset)

    val validationMetricsTolerance = 0.0000001
    MetricsAssertions.assertEssentialMetrics(model, trainingDataset, validationDataset, 1, validationMetricsTolerance)
    println("Training metrics: " + model.getMetrics(trainingDataset))
    // It seems that H2O-3 calculates training metrics for H2O-3 incorrectly
    println("Expected training metrics: " + model.getTrainingMetrics())
    println("Validation metrics: " + model.getMetrics(validationDataset))
    println("Expected validation metrics: " + model.getValidationMetrics())
    val validationMetricObject = model.getMetricsObject(validationDataset).asInstanceOf[H2OBinomialMetrics]
    val expectedValidationMetricObject = model.getValidationMetricsObject().asInstanceOf[H2OBinomialMetrics]
    TestUtils.assertDataFramesAreIdentical(
      validationMetricObject.getConfusionMatrix(),
      expectedValidationMetricObject.getConfusionMatrix())
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getThresholdsAndMetricScores(),
      expectedValidationMetricObject.getThresholdsAndMetricScores(),
      "idx",
      validationMetricsTolerance)
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getMaxCriteriaAndMetricScores(),
      expectedValidationMetricObject.getMaxCriteriaAndMetricScores(),
      "Metric",
      validationMetricsTolerance)
    validationMetricObject.getGainsLiftTable() shouldBe (null) // Gains-lift table is not supported yet.
  }

  test(s"test calculation of binomial H2OGAM metric on arbitrary dataset") {
    // Significant differences are made when number of partitions is a higher number
    val gamTrainingDataset = trainingDataset.repartition(1)
    val gamValidationDataset = validationDataset.repartition(1)
    val algorithm = new H2OGAM()
    algorithm
      .setValidationDataFrame(validationDataset)
      .setSeed(1L)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
      .setGamCols(Array("PSA"))
      .setLabelCol("CAPSULE")
    val model = algorithm.fit(gamTrainingDataset)

    assertMetrics(model, gamTrainingDataset, gamValidationDataset, 0.00001, 0.00000001)
  }
}
