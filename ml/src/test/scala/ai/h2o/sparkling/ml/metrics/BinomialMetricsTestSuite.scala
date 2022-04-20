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
import org.apache.spark.sql.functions.{rand, col}
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

  private lazy val Array(trainingDataset, validationDataset) = dataset.randomSplit(Array(0.8, 0.2), 1234L)

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
    binomialObject.getConfusionMatrix().count() should be > (0L)
    binomialObject.getConfusionMatrix().columns should not be empty
    binomialObject.getGainsLiftTable().count() should be > (0L)
    binomialObject.getGainsLiftTable().columns should not be empty
    binomialObject.getMaxCriteriaAndMetricScores().count() should be > (0L)
    binomialObject.getMaxCriteriaAndMetricScores().columns should not be empty
    binomialObject.getThresholdsAndMetricScores().count() should be > (0L)
    binomialObject.getThresholdsAndMetricScores().columns should not be empty
  }

  private def assertMetrics(
      model: H2OMOJOModel,
      trainingMetricObject: H2OBinomialMetrics,
      validationMetricObject: H2OBinomialMetrics,
      trainingMetricsTolerance: Double = 0.0,
      validationMetricsTolerance: Double = 0.0): Unit = {
    MetricsAssertions.assertEssentialMetrics(
      model,
      trainingMetricObject,
      validationMetricObject,
      trainingMetricsTolerance,
      validationMetricsTolerance)

    if (trainingMetricsTolerance < Double.PositiveInfinity) {
      val expectedTrainingMetricObject = model.getTrainingMetricsObject().asInstanceOf[H2OBinomialMetrics]

      // Confusion matrix is not correctly calculated in H2O-3 runtime.
      val trainingConfusionMatrix = trainingMetricObject.getConfusionMatrix()
      val expectedTrainingConfusionMatrix = expectedTrainingMetricObject.getConfusionMatrix()
      if (expectedTrainingConfusionMatrix == null) {
        trainingConfusionMatrix should be(null)
      } else {
        trainingConfusionMatrix.count() shouldBe >(0L)
        trainingConfusionMatrix.count() shouldEqual expectedTrainingConfusionMatrix.count()
      }

      val trainingMetricScores = trainingMetricObject.getThresholdsAndMetricScores().count()
      val expectedTrainingMetricScores = expectedTrainingMetricObject.getThresholdsAndMetricScores().count()
      trainingMetricScores shouldBe >(0L)
      trainingMetricScores shouldEqual expectedTrainingMetricScores
      TestUtils.assertDataFramesAreEqual(
        trainingMetricObject.getMaxCriteriaAndMetricScores(),
        expectedTrainingMetricObject.getMaxCriteriaAndMetricScores(),
        "Metric",
        trainingMetricsTolerance)
      trainingMetricObject.getGainsLiftTable() shouldBe (null) // Gains-lift table is not supported yet.
    }

    if (validationMetricsTolerance < Double.PositiveInfinity) {
      val expectedValidationMetricObject = model.getValidationMetricsObject().asInstanceOf[H2OBinomialMetrics]

      // Confusion matrix is not correctly calculated in H2O-3 runtime.
      val validationConfusionMatrix = validationMetricObject.getConfusionMatrix()
      val expectedValidationConfusionMatrix = expectedValidationMetricObject.getConfusionMatrix()
      if (expectedValidationConfusionMatrix == null) {
        validationConfusionMatrix should be(null)
      } else {
        validationConfusionMatrix.count() shouldBe >(0L)
        validationConfusionMatrix.count() shouldEqual expectedValidationConfusionMatrix.count()
      }

      val validationMetricScores = validationMetricObject.getThresholdsAndMetricScores().count()
      val expectedValidationMetricScores = expectedValidationMetricObject.getThresholdsAndMetricScores().count()
      validationMetricScores shouldBe >(0L)
      validationMetricScores shouldEqual expectedValidationMetricScores
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
    val algorithmsAndTolerances: Seq[(() => H2OSupervisedAlgorithm[_], Double, Double)] = Seq(
      (() => new H2ODeepLearning(), 0.00001, 0.000001),
      (() => new H2OXGBoost(), 0.0001, 0.0001),
      (() => new H2OGBM(), 0.0001, 0.0001),
      (() => new H2OGLM(), 0.00001, 0.000001),
      (() => new H2ODRF(), Double.PositiveInfinity, 0.0001))

    for ((algorithmGetter, trainingMetricsTolerance, validationMetricsTolerance) <- algorithmsAndTolerances) {
      val algorithmName = algorithmGetter().getClass.getSimpleName

      test(s"test calculation of binomial $algorithmName metrics on arbitrary dataset") {
        val algorithm = algorithmGetter()
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
          .setLabelCol("CAPSULE")

        val model = algorithm.fit(trainingDataset)

        val domain = model.getDomainValues()("CAPSULE")
        val trainingMetricObject =
          H2OBinomialMetrics.calculate(model.transform(trainingDataset), domain, labelCol = "CAPSULE")
        val validationMetricObject =
          H2OBinomialMetrics.calculate(model.transform(validationDataset), domain, labelCol = "CAPSULE")

        assertMetrics(
          model,
          trainingMetricObject,
          validationMetricObject,
          trainingMetricsTolerance,
          validationMetricsTolerance)
      }

      test(s"test calculation of binomial $algorithmName metrics with probabilities passed to predictionCol") {
        val algorithm = algorithmGetter()
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
          .setLabelCol("CAPSULE")

        val model = algorithm.fit(trainingDataset)
        val domain = model.getDomainValues()("CAPSULE")

        def extractProbability(df: DataFrame): DataFrame = {
          df.withColumn("probability", col(s"detailed_prediction.probabilities.${domain(1)}"))
        }

        val trainingMetricObject =
          H2OBinomialMetrics.calculate(
            extractProbability(model.transform(trainingDataset)),
            domain,
            labelCol = "CAPSULE",
            predictionCol = "probability")
        val validationMetricObject =
          H2OBinomialMetrics.calculate(
            extractProbability(model.transform(validationDataset)),
            domain,
            labelCol = "CAPSULE",
            predictionCol = "probability")

        assertMetrics(
          model,
          trainingMetricObject,
          validationMetricObject,
          trainingMetricsTolerance,
          validationMetricsTolerance)
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
        val domain = model.getDomainValues()("CAPSULE")
        val trainingMetricObject = H2OBinomialMetrics.calculate(
          model.transform(trainingDataset),
          domain,
          labelCol = "CAPSULE",
          weightColOption = Some("WEIGHT"))
        val validationMetricObject = H2OBinomialMetrics.calculate(
          model.transform(validationDataset),
          domain,
          labelCol = "CAPSULE",
          weightColOption = Some("WEIGHT"))

        assertMetrics(
          model,
          trainingMetricObject,
          validationMetricObject,
          trainingMetricsTolerance,
          validationMetricsTolerance)
      }
    }
  }
  {
    val algorithmsAndTolerances: Seq[(H2OSupervisedAlgorithm[_], Double, Double)] =
      Seq((new H2OXGBoost(), 0.00001, 0.00001), (new H2OGBM(), 1, 0.00001), (new H2OGLM(), 0.00001, 0.000001))

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
        val domain = model.getDomainValues()("CAPSULE")
        val trainingMetricObject = H2OBinomialMetrics.calculate(
          model.transform(trainingDataset),
          domain,
          labelCol = "CAPSULE",
          offsetColOption = Some("ID"))
        val validationMetricObject = H2OBinomialMetrics.calculate(
          model.transform(validationDataset),
          domain,
          labelCol = "CAPSULE",
          offsetColOption = Some("ID"))

        assertMetrics(
          model,
          trainingMetricObject,
          validationMetricObject,
          trainingMetricsTolerance,
          validationMetricsTolerance)
      }
    }
  }
}
