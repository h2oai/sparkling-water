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

package org.apache.spark.ml.spark.models

import java.io.File

import hex.genmodel.utils.DistributionFamily
import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OFrame
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.ml.h2o.algos.{H2ODeepLearning, H2OGBM}
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OMojoModelTest extends FunSuite with SharedH2OTestContext with Matchers {

  override def createSparkContext = new SparkContext("local[*]", "mojo-test-local", conf = defaultSparkConf)

  test("[MOJO] Export and Import - binomial model") {
    val (inputDf, model) = binomialModelFixture
    testModelReload("binomial_model_import_export", inputDf, model)
  }

  test("[MOJO] Export and Import - multinomial model") {
    val (inputDf, model) = multinomialModelFixture
    testModelReload("multinomial_model_import_export", inputDf, model)
  }

  test("[MOJO] Export and Import - regression model") {
    val (inputDf, model) = regressionModelFixture
    testModelReload("regression_model_import_export", inputDf, model)
  }

  test("[MOJO] Export and import - deep learning model") {
    val (inputDf, model) = deepLearningModelFixture
    testModelReload("deeplearning_model_import_export", inputDf, model)
  }

  // @formatter:off
  test("[MOJO] Load from mojo file - binomial model") {
    val (inputDf, mojoModel) = savedBinomialModel()
    val (_, model) = binomialModelFixture()
    assertEqual(mojoModel, model, inputDf)
  }

  test("[MOJO] Load from mojo file - multinomial model") {
    val (inputDf, mojoModel) = savedMultinomialModel()
    val (_, model) = multinomialModelFixture()
    assertEqual(mojoModel, model, inputDf)
  }

  test("[MOJO] Load from mojo file - regression model") {
    val (inputDf, mojoModel) = savedRegressionModel()
    val (_, model) = regressionModelFixture()
    assertEqual(mojoModel, model, inputDf)
  }

  test("[MOJO] Load from mojo file - deep learning model") {
    val (inputDf, mojoModel) = savedDeepLearningModel()
    val (_, model) = deepLearningModelFixture()
    assertEqual(mojoModel, model, inputDf)
  }

  test("BooleanColumn as String for mojo predictions") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("airlines_boolean.mojo"),
      "airlines_boolean.mojo")
    val data = Seq(
      Row(1987, 10, 3, "PS", 1451, "SAN", "SFO", 447, "true", "true"),
      Row(1987, 10, 4, "PS", 1451, "SAN", "SFO", 447, "false", "true"),
      Row(1987, 10, 6, "PS", 1451, "SAN", "SFO", 447, "true", "true")
    )


    val schema = StructType(List(
      StructField("Year", IntegerType, true),
      StructField("Month", IntegerType, true),
      StructField("DayOfWeek", IntegerType, true),
      StructField("UniqueCarrier", StringType, true),
      StructField("FlightNum", IntegerType, true),
      StructField("Origin", StringType, true),
      StructField("Dest", StringType, true),
      StructField("Distance", IntegerType, true),
      StructField("IsDepDelayed", StringType, true),
      StructField("IsArrDelayed", StringType, true))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    mojo.transform(df).show(3, false)
  }

  test("BooleanColumn for mojo predictions") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("airlines_boolean.mojo"),
      "airlines_boolean.mojo")
    val data = Seq(
      Row(1987, 10, 3, "PS", 1451, "SAN", "SFO", 447, true, true),
      Row(1987, 10, 4, "PS", 1451, "SAN", "SFO", 447, false, true),
      Row(1987, 10, 6, "PS", 1451, "SAN", "SFO", 447, true, true)
    )


    val schema = StructType(List(
      StructField("Year", IntegerType, true),
      StructField("Month", IntegerType, true),
      StructField("DayOfWeek", IntegerType, true),
      StructField("UniqueCarrier", StringType, true),
      StructField("FlightNum", IntegerType, true),
      StructField("Origin", StringType, true),
      StructField("Dest", StringType, true),
      StructField("Distance", IntegerType, true),
      StructField("IsDepDelayed", BooleanType, true),
      StructField("IsArrDelayed", BooleanType, true))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    mojo.transform(df).show(3, false)
  }

  test("DataFrame contains structs") {
    import spark.implicits._

    val structuredDF = prostateDataFrame.select(
      'ID,
      'CAPSULE,
      'AGE,
      struct( 'RACE, 'DPROS, struct( 'DCAPS, 'PSA) as "b") as "a",
      'VOL,
      'GLEASON)

    val expectedModel = configureGBMforProstateDF().fit(prostateDataFrame)
    val expectedPredictionDF = expectedModel.transform(prostateDataFrame).select("prediction")

    val model = configureGBMforProstateDF().fit(structuredDF)
    val predictionDF = model.transform(structuredDF).select("prediction")

    TestUtils.assertEqual(expectedPredictionDF, predictionDF)
  }

  test("Testing dataset is missing one of feature columns") {
    val Array(trainingDF, rawTestingDF) = prostateDataFrame.randomSplit(Array(0.9, 0.1))
    val testingDF = rawTestingDF
      .drop("CAPSULE", "AGE") // Remove label and one of feature columns
      .cache()
    val gbm = configureGBMforProstateDF()

    val model = gbm.fit(trainingDF)
    val predictionDF = model.transform(testingDF)

    assertPredictions(testingDF, predictionDF)
  }

  test("Testing dataset has an extra feature column") {
    val Array(trainingDF, rawTestingDF) = prostateDataFrame.randomSplit(Array(0.9, 0.1))
    val testingDF = rawTestingDF
      .drop("CAPSULE") // Remove label column
      .withColumn("EXTRA", rand()) // Add an extra column
      .cache()
    val gbm = configureGBMforProstateDF()

    val model = gbm.fit(trainingDF)
    val predictionDF = model.transform(testingDF)

    assertPredictions(testingDF, predictionDF)
  }

  def configureGBMforProstateDF(): H2OGBM = {
    new H2OGBM()
      .setNtrees(2)
      .setSeed(42)
      .setDistribution(DistributionFamily.bernoulli)
      .setLabelCol("CAPSULE")
  }

  def assertPredictions(originalDF: DataFrame, predictionDF: DataFrame): Unit ={
    val records = predictionDF.select("prediction").collect()
    val expectedNumberOfRecords = originalDF.count()
    records should have size expectedNumberOfRecords
    records.foreach { row =>
      val value = row.getAs[Row](0).toSeq.asInstanceOf[Seq[Double]]
      value should not be null
      value(0) should (be >= 0.0 and be <= 1.0)
      value(1) should (be >= 0.0 and be <= 1.0)
    }
  }

  def testModelReload(name: String, df: DataFrame, model: H2OMOJOModel): Unit = {
    val predBeforeSave = model.transform(df)
    val modelFolder = tempFolder(name)
    model.write.overwrite.save(modelFolder)
    val reloadedModel = H2OMOJOModel.load(modelFolder)
    val predAfterReload = reloadedModel.transform(df)
    // Check if predictions are same
    TestUtils.assertEqual(predBeforeSave, predAfterReload)
  }


  def assertEqual(m1: H2OMOJOModel, m2: H2OMOJOModel, df: DataFrame): Unit = {
    val predMojo = m1.transform(df)
    val predModel = m2.transform(df)

    TestUtils.assertEqual(predMojo, predModel)

  }

  def tempFolder(prefix: String) = {
    val path = java.nio.file.Files.createTempDirectory(prefix)
    path.toFile.deleteOnExit()
    path.toString
  }

  lazy val irisDataFrame = {
    hc.asDataFrame(new H2OFrame(new File(TestUtils.locate("smalldata/iris/iris_wheader.csv"))))
  }

  lazy val prostateDataFrame = {
    hc.asDataFrame(new H2OFrame(new File(TestUtils.locate("smalldata/prostate/prostate.csv"))))
  }

  def binomialModelFixture() = {
    val inputDf = prostateDataFrame
    val gbm = new H2OGBM()
      .setNtrees(2)
      .setSeed(42)
      .setDistribution(DistributionFamily.bernoulli)
      .setLabelCol("capsule")

    (inputDf, gbm.fit(inputDf))
  }

  def multinomialModelFixture() = {
    val inputDf = irisDataFrame
    val gbm = new H2OGBM()
      .setNtrees(2)
      .setSeed(42)
      .setDistribution(DistributionFamily.multinomial)
      .setLabelCol("class")

    (inputDf, gbm.fit(inputDf))
  }

  def regressionModelFixture() = {
    val inputDf = prostateDataFrame
    val gbm = new H2OGBM()
      .setNtrees(2)
      .setSeed(42)
      .setLabelCol("capsule")

    (inputDf, gbm.fit(inputDf))
  }

  def deepLearningModelFixture() = {
    val inputDf = prostateDataFrame
    val dl = new H2ODeepLearning()
      .setSeed(42)
      .setReproducible(true)
      .setLabelCol("CAPSULE")

    (inputDf, dl.fit(inputDf))
  }

  def savedBinomialModel() = {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("binom_model_prostate.mojo"),
      "binom_model_prostate.mojo")
    (prostateDataFrame, mojo)
  }

  def savedRegressionModel() = {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("regre_model_prostate.mojo"),
      "regre_model_prostate.mojo")
    (prostateDataFrame, mojo)
  }

  def savedMultinomialModel() = {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("multi_model_iris.mojo"),
      "multi_model_iris.mojo")
    (irisDataFrame, mojo)
  }

  def savedDeepLearningModel() = {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("deep_learning_prostate.mojo"),
      "deep_learning_prostate.mojo")
    (prostateDataFrame, mojo)
  }

}
