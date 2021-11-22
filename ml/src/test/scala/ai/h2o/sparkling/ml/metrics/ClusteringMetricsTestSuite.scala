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

import ai.h2o.sparkling.ml.algos.H2OKMeans
import ai.h2o.sparkling.ml.models.{H2OKMeansMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ClusteringMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))

  private lazy val Array(trainingDataset, validationDataset) = dataset.randomSplit(Array(0.8, 0.2), seed = 42)

  test("test calculation of kmeans metric objects on arbitrary dataset") {
    val algorithm = new H2OKMeans()
      .setValidationDataFrame(validationDataset)
      .setSeed(1)
      .setK(3)
      .setUserPoints(Array(Array(4.9, 3.0, 1.4, 0.2), Array(5.6, 2.5, 3.9, 1.1), Array(6.5, 3.0, 5.2, 2.0)))
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algorithm.fit(trainingDataset)

    assertMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance = 0.00001)
  }

  private def assertMetrics(
      model: H2OKMeansMOJOModel,
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
    TestUtils.assertDataFramesAreEqual(
      model.getTrainingMetricsObject().getCentroidStats(),
      model.getMetricsObject(trainingDataset).getCentroidStats(),
      "Centroid",
      Map("Size" -> trainingMetricsTolerance, "Within Cluster Sum of Squares" -> trainingMetricsTolerance))
    TestUtils.assertDataFramesAreEqual(
      model.getValidationMetricsObject().getCentroidStats(),
      model.getMetricsObject(validationDataset).getCentroidStats(),
      "Centroid",
      Map("Size" -> validationMetricsTolerance, "Within Cluster Sum of Squares" -> validationMetricsTolerance))
  }
}
