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
import org.apache.spark.sql.functions.rand
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
    .withColumn("RACE", 'RACE.cast(StringType))
    .withColumn("DCAPS", 'DCAPS.cast(StringType))
    .withColumn("WEIGHT", rand(42))
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

    if (trainingMetricsTolerance < Double.PositiveInfinity) {
      val trainingMetricObject = model.getMetricsObject(trainingDataset).asInstanceOf[H2OBinomialMetrics]
      val expectedTrainingMetricObject = model.getTrainingMetricsObject().asInstanceOf[H2OBinomialMetrics]

      // Confusion matrix is not correctly calculated in H2O-3 runtime.
      val trainingConfusionMatrix = trainingMetricObject.getConfusionMatrix().count()
      val expectedTrainingConfusionMatrix = expectedTrainingMetricObject.getConfusionMatrix().count()
      trainingConfusionMatrix shouldBe >(0L)
      trainingConfusionMatrix shouldEqual expectedTrainingConfusionMatrix

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
    }

    if (validationMetricsTolerance < Double.PositiveInfinity) {
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
    val algorithmsAndTolerances: Seq[(() => H2OSupervisedAlgorithm[_], Double, Double, Boolean)] = Seq(
      (() => new H2ODeepLearning(), 0.00001, 0.000001, false),
      (() => new H2OXGBoost(), 0.0001, 0.0001, false),
      (() => new H2OGBM(), 0.0001, 0.0001, false),
      (() => new H2OGLM(), 0.00001, 0.000001, false),
      (() => new H2ODRF(), Double.PositiveInfinity, 0.0001, false),
      (() => new H2ORuleFit(), 0.00001, 0.00001, true))

    for ((algorithmGetter, trainingMetricsTolerance, validationMetricsTolerance, skipExtraMetrics) <- algorithmsAndTolerances) {
      val algorithmName = algorithmGetter().getClass.getSimpleName

      test(s"test calculation of binomial $algorithmName metrics on arbitrary dataset") {
        val algorithm = algorithmGetter()
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
          .setLabelCol("CAPSULE")
        val model = algorithm.fit(trainingDataset)

        assertMetrics(
          model,
          trainingDataset,
          validationDataset,
          trainingMetricsTolerance,
          validationMetricsTolerance,
          skipExtraMetrics)
      }

      test(s"test calculation of binomial $algorithmName metrics with weightCol set on arbitrary dataset") {
        val algorithm = algorithmGetter()
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
          .setLabelCol("CAPSULE")
          .setWeightCol("WEIGHT")
        val model = algorithm.fit(trainingDataset)

        assertMetrics(
          model,
          trainingDataset,
          validationDataset,
          trainingMetricsTolerance,
          validationMetricsTolerance,
          skipExtraMetrics)
      }
    }
  }
  {
    val algorithmsAndTolerances: Seq[(H2OSupervisedAlgorithm[_], Double, Double)] =
      Seq((new H2OXGBoost(), 0.00001, 0.00001), (new H2OGBM(), 0.0001, 0.00001), (new H2OGLM(), 0.00001, 0.000001))

    for ((algorithm, trainingMetricsTolerance, validationMetricsTolerance) <- algorithmsAndTolerances) {
      val algorithmName = algorithm.getClass.getSimpleName

      test(s"test calculation of binomial $algorithmName metrics with offsetCol set on arbitrary dataset") {
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
          .setLabelCol("CAPSULE")
          .setOffsetCol("ID")
        val model = algorithm.fit(trainingDataset)

        assertMetrics(model, trainingDataset, validationDataset, trainingMetricsTolerance, validationMetricsTolerance)
      }
    }
  }

  {
    // TODO: Investigate differences when data frames have more partitions
    def gamTrainingDataset = trainingDataset.repartition(1)
    def gamValidationDataset = validationDataset.repartition(1)

    test(s"test calculation of binomial H2OGAM metrics on arbitrary dataset") {
      val algorithm = new H2OGAM()
      algorithm
        .setValidationDataFrame(gamValidationDataset)
        .setSeed(1L)
        .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
        .setGamCols(Array("PSA"))
        .setLabelCol("CAPSULE")
      val model = algorithm.fit(gamTrainingDataset)

      assertMetrics(model, gamTrainingDataset, gamValidationDataset, 0.00001, 0.00000001)
    }

    // H2OGAM renames Gam cols when offset columns is set (PSA -> PSA_0_center__8)
    ignore(s"test calculation of binomial H2OGAM metrics with offsetCol set on arbitrary dataset") {
      val algorithm = new H2OGAM()
      algorithm
        .setValidationDataFrame(gamValidationDataset)
        .setSeed(1L)
        .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
        .setGamCols(Array("PSA"))
        .setLabelCol("CAPSULE")
        .setOffsetCol("ID")
      val model = algorithm.fit(gamTrainingDataset)

      assertMetrics(model, gamTrainingDataset, gamValidationDataset, 0.00001, 0.00000001)
    }

    test(s"test calculation of binomial H2OGAM metrics with weightCol set on arbitrary dataset") {
      val algorithm = new H2OGAM()
      algorithm
        .setValidationDataFrame(gamValidationDataset)
        .setSeed(1L)
        .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
        .setGamCols(Array("PSA"))
        .setLabelCol("CAPSULE")
        .setWeightCol("ID")
      val model = algorithm.fit(gamTrainingDataset)

      assertMetrics(model, gamTrainingDataset, gamValidationDataset, 0.00001, 0.00000001)
    }
  }
}
