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

import ai.h2o.sparkling.ml.algos.{H2OGAM, H2OGLM}
import ai.h2o.sparkling.ml.models.{H2OGLMMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class OrdinalMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/insurance.csv"))

  private lazy val Array(trainingDataset, validationDataset) = dataset.randomSplit(Array(0.8, 0.2))

  private def assertMetrics[T](model: H2OMOJOModel): Unit = {
    assertMetrics(model.getTrainingMetricsObject(), model.getTrainingMetrics())
    assertMetrics(model.getValidationMetricsObject(), model.getValidationMetrics())
    assert(model.getCrossValidationMetricsObject() == null)
    assert(model.getCrossValidationMetrics() == Map())
  }

  private def assertMetrics(metricsObject: H2OMetrics, metrics: Map[String, Double]): Unit = {
    metricsObject.isInstanceOf[H2OOrdinalGLMMetrics] should be(true)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(metricsObject, metrics)
  }

  test("test ordinal glm metric objects") {
    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setFeaturesCols("District", "Group", "Claims")
      .setLabelCol("Age")
      .setSeed(1)
      .setFamily("ordinal")
    val model = algo.fit(dataset)
    assertMetrics[H2OOrdinalMetrics](model)

    model.write.overwrite().save("ml/build/glm_ordinal_model_metrics")
    val loadedModel = H2OGLMMOJOModel.load("ml/build/glm_ordinal_model_metrics")
    assertMetrics[H2OOrdinalGLMMetrics](loadedModel)
  }

  test("test calculation of ordinal glm metric objects on arbitrary dataset") {
    val algo = new H2OGLM()
      .setValidationDataFrame(validationDataset)
      .setFeaturesCols("District", "Group", "Claims")
      .setLabelCol("Age")
      .setSeed(1)
      .setFamily("ordinal")
    val model = algo.fit(trainingDataset)

    assertMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance = 0.00001)
  }

  test("test calculation of ordinal gam metric objects on arbitrary dataset") {
    val algo = new H2OGAM()
      .setValidationDataFrame(validationDataset)
      .setFeaturesCols("District", "Group")
      .setGamCols(Array("Claims"))
      .setLabelCol("Age")
      .setSeed(1)
      .setFamily("ordinal")
    val model = algo.fit(trainingDataset)

    assertMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance = 0.00001)
  }

  private def assertMetrics(
      model: H2OMOJOModel,
      trainingDataset: DataFrame,
      validationDataset: DataFrame,
      trainingMetricsTolerance: Double = 0.0,
      validationMetricsTolerance: Double = 0.0): Unit = {
    MetricsAssertions.assertEssentialMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance,
      validationMetricsTolerance)

    val trainingMetricObject = model.getTrainingMetricsObject().asInstanceOf[H2OOrdinalGLMMetrics]
    val expectedTrainingMetricObject = model.getMetricsObject(trainingDataset).asInstanceOf[H2OOrdinalGLMMetrics]
    TestUtils.assertDataFramesAreIdentical(
      trainingMetricObject.getConfusionMatrix(),
      expectedTrainingMetricObject.getConfusionMatrix())
    TestUtils.assertDataFramesAreEqual(
      trainingMetricObject.getHitRatioTable(),
      expectedTrainingMetricObject.getHitRatioTable(),
      "K",
      Map("Hit Ratio" -> trainingMetricsTolerance))

    val validationMetricObject = model.getValidationMetricsObject().asInstanceOf[H2OOrdinalGLMMetrics]
    val expectedValidationMetricObject = model.getMetricsObject(validationDataset).asInstanceOf[H2OOrdinalGLMMetrics]
    TestUtils.assertDataFramesAreIdentical(
      validationMetricObject.getConfusionMatrix(),
      expectedValidationMetricObject.getConfusionMatrix())
    TestUtils.assertDataFramesAreEqual(
      validationMetricObject.getHitRatioTable(),
      expectedValidationMetricObject.getHitRatioTable(),
      "K",
      Map("Hit Ratio" -> validationMetricsTolerance))
  }
}
