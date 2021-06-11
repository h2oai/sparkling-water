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

package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.algos.H2ODeepLearning
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OAutoEncoderTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("CAPSULE", 'CAPSULE.cast("string"))
    .withColumn("RACE", 'RACE.cast("string"))

  private lazy val Array(trainingDataset, testingDataset) = dataset.randomSplit(Array(.8, .2), 42)

  private lazy val standaloneModel = {
    val algo = new H2OAutoEncoder()
      .setSeed(1)
      .setInputCols("DCAPS", "PSA", "VOL")
      .setOutputCol("Output")
      .setWithOriginalCol(true)
      .setOriginalCol("Original")
      .setWithMSECol(true)
      .setMSECol("MSE")
      .setHidden(Array(3))
      .setSplitRatio(0.8)
      .setReproducible(true)

    algo.fit(trainingDataset)
  }

  test("Standalone AutoEncoder produces output, original and mse columns") {
    val scored = standaloneModel.transform(testingDataset)
    val firstRow = scored.first()

    val output = firstRow.getAs[DenseVector]("Output")
    output.values.length shouldBe 3
    output.values(0) shouldBe (1.1119691119691115 +- 0.0001)
    output.values(1) shouldBe (33.512407658954146 +- 0.0001)
    output.values(2) shouldBe (15.305752895752896 +- 0.0001)

    val original = firstRow.getAs[DenseVector]("Original")
    original.values.length shouldBe 3
    original.values(0) shouldBe (2.0 +- 0.0001)
    original.values(1) shouldBe (4.9 +- 0.0001)
    original.values(2) shouldBe (0.0 +- 0.0001)

    val mse = firstRow.getAs[Double]("MSE")
    mse shouldBe (0.2863796079729097 +- 0.0001)
  }

  test("Standalone AutoEncoder can produce training and validation metrics") {
    val trainingMetrics = standaloneModel.getTrainingMetrics()
    trainingMetrics.get("MSE").get shouldBe (0.04592776534626871 +- 0.0001)
    trainingMetrics.get("RMSE").get shouldBe (0.2143076418289108 +- 0.0001)

    val validationMetrics = standaloneModel.getValidationMetrics()
    validationMetrics.get("MSE").get shouldBe (0.031939404011711074 +- 0.0001)
    validationMetrics.get("RMSE").get shouldBe (0.17871598700651006 +- 0.0001)
  }

  test("Standalone AutoEncoder provide scoring history") {
    val expectedColumns = Array(
      "Timestamp",
      "Duration",
      "Training Speed",
      "Epochs",
      "Iterations",
      "Samples",
      "Training RMSE",
      "Training MSE",
      "Validation RMSE",
      "Validation MSE")

    val scoringHistoryDF = standaloneModel.getScoringHistory()
    scoringHistoryDF.count() shouldBe >(0L)
    scoringHistoryDF.columns shouldEqual expectedColumns
  }

  test("Standalone AutoEncoder provide feature importances") {
    val expectedColumns = Array("Variable", "Relative Importance", "Scaled Importance", "Percentage")

    val featureImportancesDF = standaloneModel.getFeatureImportances()
    featureImportancesDF.count() shouldEqual standaloneModel.getInputCols().length
    featureImportancesDF.columns shouldEqual expectedColumns
  }
}
