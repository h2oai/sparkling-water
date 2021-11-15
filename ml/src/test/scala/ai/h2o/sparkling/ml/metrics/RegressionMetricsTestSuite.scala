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
import ai.h2o.sparkling.ml.algos._
import ai.h2o.sparkling.ml.models.{H2OGBMMOJOModel, H2OGLMMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class RegressionMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("RACE", 'RACE.cast(StringType))
    .withColumn("DCAPS", 'DCAPS.cast(StringType))
    .repartition(20)

  private lazy val Array(trainingDataset, validationDataset) = dataset.randomSplit(Array(0.8, 0.2), 42L)

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

  {
    val algorithmsAndTolerances: Seq[(() => H2OSupervisedAlgorithm[_], Double, Double, Boolean)] = Seq(
      (() => new H2ODeepLearning(), 0.00001, 0.00000001, false),
      (() => new H2OXGBoost(), 0.00001, 0.00000001, false),
      (() => new H2OGBM(), 0.0001, 0.00000001, false),
      (() => new H2OGLM(), 0.00001, 0.00000001, false),
      (() => new H2ODRF(), Double.PositiveInfinity, 0.00000001, false)) // ignore comparision on the training dataset
    // H2O runtime produces additional GLM metrics
    // TODO: investigate differences  (() => new H2ORuleFit(), 0.001, 0.00000001, true))

    for ((algorithmGetter, trainingMetricsTolerance, validationMetricsTolerance, skipExtraMetrics) <- algorithmsAndTolerances) {
      val algorithmName = algorithmGetter().getClass.getSimpleName

      test(s"test calculation of regression $algorithmName metrics on arbitrary dataset") {
        val algorithm = algorithmGetter()
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
          .setLabelCol("AGE")
        val model = algorithm.fit(trainingDataset)

        MetricsAssertions.assertEssentialMetrics(
          model,
          trainingDataset,
          validationDataset,
          trainingMetricsTolerance,
          validationMetricsTolerance,
          skipExtraMetrics)
      }

      test(s"test calculation of regression $algorithmName metrics with weight column set on arbitrary dataset ") {
        val algorithm = algorithmGetter()
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
          .setLabelCol("AGE")
          .setWeightCol("ID")
        val model = algorithm.fit(trainingDataset)

        MetricsAssertions.assertEssentialMetrics(
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
    val algorithmsAndTolerances: Seq[(H2OSupervisedAlgorithm[_], Double, Double)] = Seq(
      (new H2OXGBoost(), 0.00001, 0.00000001),
      (new H2OGBM(), 0.001, 0.00000001),
      (new H2OGLM(), 0.00001, 0.00000001)) // H2ORuleFit and H2ODRF doesn't support offset column

    for ((algorithm, trainingMetricsTolerance, validationMetricsTolerance) <- algorithmsAndTolerances) {
      val algorithmName = algorithm.getClass.getSimpleName
      test(s"test calculation of regression $algorithmName metrics with offset column set on arbitrary dataset ") {
        algorithm
          .setValidationDataFrame(validationDataset)
          .set(algorithm.getParam("seed"), 1L)
          .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
          .setLabelCol("AGE")
          .setOffsetCol("ID")
        val model = algorithm.fit(trainingDataset)

        MetricsAssertions.assertEssentialMetrics(
          model,
          trainingDataset,
          validationDataset,
          trainingMetricsTolerance,
          validationMetricsTolerance)
      }
    }
  }
  {
    // TODO: investigate why GAM there are differences when more partitions are used
    def gamTrainingDataset = trainingDataset.repartition(1)
    def gamValidationDataset = validationDataset.repartition(1)

    test(s"test calculation of regression H2OGAM metrics on arbitrary dataset") {
      val algorithm = new H2OGAM()
      algorithm
        .setValidationDataFrame(gamValidationDataset)
        .setSeed(1L)
        .setGamCols(Array(Array("PSA")))
        .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
        .setLabelCol("AGE")
      val model = algorithm.fit(gamTrainingDataset)

      MetricsAssertions.assertEssentialMetrics(model, gamTrainingDataset, gamValidationDataset, 0.00001, 0.00000001)
    }

    test(s"test calculation of regression H2OGAM metrics with weight column set on arbitrary dataset") {
      val algorithm = new H2OGAM()
      algorithm
        .setValidationDataFrame(gamValidationDataset)
        .setSeed(1L)
        .setGamCols(Array(Array("PSA")))
        .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
        .setLabelCol("AGE")
        .setWeightCol("ID")
      val model = algorithm.fit(gamTrainingDataset)

      MetricsAssertions.assertEssentialMetrics(model, gamTrainingDataset, gamValidationDataset, 0.00001, 0.00000001)
    }

    // H2OGAM renames Gam cols when offset columns is set (PSA -> PSA_0_center__8)
    ignore(s"test calculation of regression H2OGAM metrics with offset column set on arbitrary dataset") {
      val algorithm = new H2OGAM()
      algorithm
        .setValidationDataFrame(gamValidationDataset)
        .setSeed(1L)
        .setGamCols(Array(Array("PSA")))
        .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
        .setLabelCol("AGE")
        .setOffsetCol("ID")
      val model = algorithm.fit(gamTrainingDataset)

      MetricsAssertions.assertEssentialMetrics(model, gamTrainingDataset, gamValidationDataset, 0.00001, 0.00000001)
    }
  }
}
