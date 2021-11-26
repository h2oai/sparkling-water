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

import ai.h2o.sparkling.ml.features.{H2OGLRM, H2OPCA}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class DimReductionMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
  private lazy val Array(trainingDataset, validationDataset) = dataset.randomSplit(Array(0.8, 0.2), seed = 42)

  test("test calculation of H2OPCA metrics on arbitrary dataset") {
    val algorithm = new H2OPCA()
      .setSeed(1)
      .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setOutputCol("Output")
      .setValidationDataFrame(validationDataset)
      .setImputeMissing(true)
      .setPcaMethod("Power")
      .setK(3)

    val model = algorithm.fit(trainingDataset)

    MetricsAssertions.assertEssentialMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance = 0.00001,
      validationMetricsTolerance = 0.00001)
  }

  test("test calculation of H2OGLRM metrics on arbitrary dataset") {
    val Array(rawTrainingDataset, rawValidationDataset) = dataset.randomSplit(Array(0.5, 0.5), seed = 42)
    val glrmTrainingDataset = rawTrainingDataset.limit(150)
    val glrmValidationDataset = rawValidationDataset.limit(150)

    val algorithm = new H2OGLRM()
      .setSeed(1)
      .setValidationDataFrame(glrmValidationDataset)
      .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setOutputCol("Output")
      .setWithReconstructedCol(true)
      .setReconstructedCol("Reconstructed")
      .setK(3)

    val model = algorithm.fit(glrmTrainingDataset)
    println(model.getTrainingMetrics())
    println(model.getMetrics(glrmTrainingDataset))
    println(model.getValidationMetrics())
    println(model.getMetrics(glrmValidationDataset))
    MetricsAssertions.assertEssentialMetrics(
      model,
      glrmTrainingDataset,
      glrmValidationDataset,
      trainingMetricsTolerance = Double.PositiveInfinity,
      validationMetricsTolerance = 0.00001)
  }
}
