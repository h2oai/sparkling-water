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

import ai.h2o.sparkling.ml.algos.H2OGBM
import ai.h2o.sparkling.ml.metrics.{H2OAutoEncoderMetrics, MetricsAssertions}
import ai.h2o.sparkling.ml.models.{H2OAutoEncoderMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
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

  private lazy val trainingDataset = dataset.limit(300).cache()
  private lazy val testingDataset = dataset.except(trainingDataset).cache()

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

  test("Standalone auto encoder can produce output, original and mse columns") {
    val scored = standaloneModel.transform(testingDataset)
    val firstRow = scored.first()

    val output = firstRow.getAs[DenseVector]("Output")
    output.values.length shouldBe 3
    output.values(0) shouldBe (1.1249999999999996 +- 0.0001)
    output.values(1) shouldBe (16.512416666666667 +- 0.0001)
    output.values(2) shouldBe (23.01268813674309 +- 0.0001)

    val original = firstRow.getAs[DenseVector]("Original")
    original.values.length shouldBe 3
    original.values(0) shouldBe (1.0 +- 0.0001)
    original.values(1) shouldBe (3.4 +- 0.0001)
    original.values(2) shouldBe (30.8 +- 0.0001)

    val mse = firstRow.getAs[Double]("MSE")
    mse shouldBe (0.011028919003303739 +- 0.0001)
  }

  test("Standalone AutoEncoder can produce training and validation metrics") {
    val trainingMetrics = standaloneModel.getTrainingMetrics()
    trainingMetrics.get("MSE").get shouldBe (0.030036871130634916 +- 0.0001)
    trainingMetrics.get("RMSE").get shouldBe (0.1733114858589439 +- 0.0001)

    val validationMetrics = standaloneModel.getValidationMetrics()
    validationMetrics.get("MSE").get shouldBe (0.017766277571512626 +- 0.0001)
    validationMetrics.get("RMSE").get shouldBe (0.133290200583211 +- 0.0001)
  }

  test("Standalone autoencoder can provide scoring history") {
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

  test("Standalone auto encoder can provide feature importances") {
    val expectedColumns = Array("Variable", "Relative Importance", "Scaled Importance", "Percentage")

    val featureImportancesDF = standaloneModel.getFeatureImportances()
    featureImportancesDF.count() shouldEqual standaloneModel.getInputCols().length
    featureImportancesDF.columns shouldEqual expectedColumns
  }

  test("Old auto encoder MOJO model can score and produce output, original and mse columns") {
    val columns = (1 to 210).map(i => StructField("C" + i, DoubleType, nullable = false))

    val df = spark.read
      .schema(StructType(columns))
      .csv(TestUtils.locate("smalldata/anomaly/ecg_discord_test.csv"))

    val mojoName: String = "deep_learning_auto_encoder.mojo"
    val mojoStream = this.getClass.getClassLoader.getResourceAsStream(mojoName)
    val settings = H2OMOJOSettings(convertInvalidNumbersToNa = false, convertUnknownCategoricalLevelsToNa = false)
    val mojo = H2OAutoEncoderMOJOModel.createFromMojo(mojoStream, mojoName, settings)

    mojo.setOutputCol("Output")
    mojo.setOriginalCol("Original")
    mojo.setWithOriginalCol(true)
    mojo.setMSECol("MSE")
    mojo.setWithMSECol(true)

    val result = mojo.transform(df)
    val firstRow = result.first()

    val output = firstRow.getAs[DenseVector]("Output")
    output.values.length shouldBe 210

    val original = firstRow.getAs[DenseVector]("Original")
    original.values.length shouldBe 210

    val mse = firstRow.getAs[Double]("MSE")
    mse shouldBe (0.01838996923742329 +- 0.0001)
  }

  test("The auto encoder is able to transform dataset after it's saved and loaded") {
    val autoEncoder = new H2OAutoEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setOutputCol("Output")
      .setWithOriginalCol(true)
      .setOriginalCol("Original")
      .setWithMSECol(true)
      .setMSECol("MSE")
      .setHidden(Array(3))
      .setSplitRatio(0.8)
      .setReproducible(true)

    val pipeline = new Pipeline().setStages(Array(autoEncoder))

    val model = pipeline.fit(trainingDataset)
    val expectedTestingDataset = model.transform(testingDataset)
    val path = "build/ml/autoEncoder_save_load"
    model.write.overwrite().save(path)
    val loadedModel = PipelineModel.load(path)
    val transformedTestingDataset = loadedModel.transform(testingDataset)

    TestUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedTestingDataset)
  }

  test("A pipeline with an auto encoder transforms testing dataset without an exception") {
    val autoEncoder = new H2OAutoEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setHidden(Array(3))

    val gbm = new H2OGBM()
      .setFeaturesCol(autoEncoder.getOutputCol())
      .setLabelCol("CAPSULE")

    val pipeline = new Pipeline().setStages(Array(autoEncoder, gbm))

    val model = pipeline.fit(trainingDataset)
    val rows = model.transform(testingDataset).groupBy("prediction").count().collect()
    rows.foreach { row =>
      assert(row.getAs[Long]("count") > 0, s"No predictions of class '${row.getAs[Int]("prediction")}'")
    }
  }

  private def assertMetrics(model: H2OAutoEncoderMOJOModel): Unit = {
    assertMetrics(model.getTrainingMetricsObject(), model.getTrainingMetrics())
    assertMetrics(model.getValidationMetricsObject(), model.getValidationMetrics())
    assert(model.getCrossValidationMetricsObject() == null)
    assert(model.getCrossValidationMetrics() == Map())
  }

  private def assertMetrics(metricsObject: H2OAutoEncoderMetrics, metrics: Map[String, Double]): Unit = {
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(metricsObject, metrics)
  }

  test("test metric objects") {
    assertMetrics(standaloneModel)

    standaloneModel.write.overwrite().save("ml/build/ae_model_metrics")
    val loadedModel = H2OAutoEncoderMOJOModel.load("ml/build/ae_model_metrics")
    assertMetrics(loadedModel)
  }
}
