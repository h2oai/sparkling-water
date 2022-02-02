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

package ai.h2o.sparkling.ml.models

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import ai.h2o.sparkling.ml.algos.classification.H2OGBMClassifier
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import ai.h2o.sparkling.ml.algos.{H2ODeepLearning, H2OGBM, H2OGLM}
import ai.h2o.sparkling.ml.metrics.H2OBinomialMetrics
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OMOJOModelTestSuite extends FunSuite with SharedH2OTestContext with Matchers {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  test("DataFrame-based parameters on H2OMOJOModel are Java serializable") {
    System.setProperty("spark.testing", "false") // To enable serialization on H2OMOJOModel
    val estimator = new H2OGBMClassifier()
      .setSeed(1)
      .setNfolds(3)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
    val model = estimator.fit(prostateDataFrame)

    val serialized = withResource(new ByteArrayOutputStream()) { byteStream =>
      withResource(new ObjectOutputStream(byteStream)) { objectStream =>
        objectStream.writeObject(model)
        byteStream.flush()
        byteStream.toByteArray
      }
    }
    val deserializedModel = withResource(new ByteArrayInputStream(serialized)) { byteStream =>
      withResource(new ObjectInputStream(byteStream)) { objectStream =>
        objectStream.readObject().asInstanceOf[H2OMOJOModel]
      }
    }

    def assertDataframesOnMOJOModels(expected: H2OMOJOModel, deserialized: H2OMOJOModel): Unit = {
      deserialized shouldNot be(null)

      val expectedFeatureImportances = expected.getFeatureImportances()
      val expectedScoringHistory = expected.getScoringHistory()
      val expectedTrainingMetrics = expected.getTrainingMetricsObject().asInstanceOf[H2OBinomialMetrics]
      val expectedThresholdsAndMetricScores = expectedTrainingMetrics.getThresholdsAndMetricScores()
      val expectedMaxCriteriaAndMetricScores = expectedTrainingMetrics.getMaxCriteriaAndMetricScores()
      val expectedGainsLiftTable = expectedTrainingMetrics.getGainsLiftTable()
      val expectedConfusionMatrix = expectedTrainingMetrics.getConfusionMatrix()

      val featureImportances = deserialized.getFeatureImportances()
      val scoringHistory = deserialized.getScoringHistory()
      val trainingMetrics = expected.getTrainingMetricsObject().asInstanceOf[H2OBinomialMetrics]
      val thresholdsAndMetricScores = trainingMetrics.getThresholdsAndMetricScores()
      val maxCriteriaAndMetricScores = trainingMetrics.getMaxCriteriaAndMetricScores()
      val gainsLiftTable = expectedTrainingMetrics.getGainsLiftTable()
      val confusionMatrix = expectedTrainingMetrics.getConfusionMatrix()

      featureImportances shouldNot be(null)
      scoringHistory shouldNot be(null)
      thresholdsAndMetricScores shouldNot be(null)
      maxCriteriaAndMetricScores shouldNot be(null)
      gainsLiftTable shouldNot be(null)
      confusionMatrix shouldNot be(null)

      TestUtils.assertDataFramesAreIdentical(expectedFeatureImportances, featureImportances)
      TestUtils.assertDataFramesAreIdentical(expectedScoringHistory, scoringHistory)
      TestUtils.assertDataFramesAreIdentical(expectedThresholdsAndMetricScores, thresholdsAndMetricScores)
      TestUtils.assertDataFramesAreIdentical(expectedMaxCriteriaAndMetricScores, maxCriteriaAndMetricScores)
      TestUtils.assertDataFramesAreIdentical(expectedGainsLiftTable, gainsLiftTable)
      TestUtils.assertDataFramesAreIdentical(expectedConfusionMatrix, confusionMatrix)
    }

    val expectedCVModels = model.getCrossValidationModels()
    val expectedCVScoringHistory = model.getCrossValidationModelsScoringHistory()
    val expectedCrossValidationSummary = model.getCrossValidationMetricsSummary()

    val crossValidationSummary = deserializedModel.getCrossValidationMetricsSummary()
    val deserializedCVModels = deserializedModel.getCrossValidationModels()
    val deserializedCVScoringHistory = deserializedModel.getCrossValidationModelsScoringHistory()
    TestUtils.assertDataFramesAreIdentical(expectedCrossValidationSummary, crossValidationSummary)
    assertDataframesOnMOJOModels(model, deserializedModel)

    for (i <- expectedCVModels.indices) {
      assertDataframesOnMOJOModels(expectedCVModels(i), deserializedCVModels(i))
      deserializedCVScoringHistory(i) shouldNot be(null)
      TestUtils.assertDataFramesAreIdentical(expectedCVScoringHistory(i), deserializedCVScoringHistory(i))
    }
  }

  test("H2OMOJOModel saved with scala 2.11 behaves the same way as H2OMOJOModel saved with scala 2.12") {
    val model11 = H2OMOJOModel.load("ml/src/test/resources/sw_mojo_scala_2.11_df_java_serde")
    val model12 = H2OMOJOModel.load("ml/src/test/resources/sw_mojo_scala_2.12_df_java_serde")
    compareMOJOModels(model11, model12)
  }

  test("H2OMOJOModel saved with current serialization behaves the same way as old models") {
    val model11 = H2OMOJOModel.load("ml/src/test/resources/sw_mojo_scala_2.11_df_java_serde")
    val path = "ml/build/mojo_model_serialization_compatibility"
    model11.write.overwrite.save(path)
    val currentModel = H2OMOJOModel.load(path)
    compareMOJOModels(model11, currentModel)
  }

  private def compareMOJOModels(first: H2OMOJOModel, second: H2OMOJOModel) = {
    TestUtils.assertDataFramesAreIdentical(first.transform(prostateDataFrame), second.transform(prostateDataFrame))
    TestUtils.assertDataFramesAreIdentical(first.getFeatureImportances(), second.getFeatureImportances())
    TestUtils.assertDataFramesAreIdentical(
      first.getCrossValidationMetricsSummary(),
      second.getCrossValidationMetricsSummary())
    first.getTrainingMetrics().-("ScoringTime") shouldEqual second.getTrainingMetrics().-("ScoringTime")
  }

  test("[MOJO] Export and Import - binomial model") {
    val (inputDf, model) = binomialModelFixture()
    testModelReload("binomial_model_import_export", inputDf, model)
  }

  test("[MOJO] Export and Import - multinomial model") {
    val (inputDf, model) = multinomialModelFixture()
    testModelReload("multinomial_model_import_export", inputDf, model)
  }

  test("[MOJO] Export and Import - regression model") {
    val (inputDf, model) = regressionModelFixture()
    testModelReload("regression_model_import_export", inputDf, model)
  }

  test("[MOJO] Export and import - deep learning model") {
    val (inputDf, model) = deepLearningModelFixture()
    testModelReload("deeplearning_model_import_export", inputDf, model)
  }

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

  test("should not fail when handling boolean column also when loading mojo from file") {
    val prostateDFWithBooleanColumn =
      prostateDataFrame.withColumn("DCAPS", when($"DCAPS".equalTo("2"), true).otherwise(false))
    val Array(trainingDF, testingDF) = prostateDFWithBooleanColumn.randomSplit(Array(0.9, 0.1))
    val model = configureGBMForProstateDF().fit(trainingDF)

    testModelReload("model_with_boolean_column", testingDF, model)
  }

  def compareGbmOnTwoDatasets(reference: DataFrame, tested: DataFrame) = {
    val columnsForComparison =
      Seq($"prediction", $"detailed_prediction.probabilities.0", $"detailed_prediction.probabilities.1")

    val expectedModel = configureGBMForProstateDF().fit(reference)
    val expectedPredictionDF = expectedModel.transform(reference).select(columnsForComparison: _*)

    val model = configureGBMForProstateDF().fit(tested)
    val predictionDF = model.transform(tested).select(columnsForComparison: _*)

    TestUtils.assertDataFramesAreIdentical(expectedPredictionDF, predictionDF)
  }

  test("DataFrame contains structs") {
    val structuredDataFrame = prostateDataFrame.select(
      'ID,
      'CAPSULE,
      'AGE,
      struct('RACE, 'DPROS, struct('DCAPS, 'PSA) as "b") as "a",
      'VOL,
      'GLEASON)

    compareGbmOnTwoDatasets(prostateDataFrame, structuredDataFrame)
  }

  def prostateDataFrameWithDoubles =
    prostateDataFrame.select(
      'CAPSULE cast "string" as "CAPSULE",
      'AGE cast "double" as "AGE",
      'RACE cast "double" as "RACE",
      'DPROS cast "double" as "DPROS",
      'DCAPS cast "double" as "DCAPS",
      'PSA,
      'VOL,
      'GLEASON cast "double" as "GLEASON")

  test("DataFrame contains array") {
    val arrayDataFrame = prostateDataFrameWithDoubles.select(
      'CAPSULE,
      array('AGE, 'RACE, 'DPROS, 'DCAPS, 'PSA, 'VOL, 'GLEASON) as "features")

    compareGbmOnTwoDatasets(prostateDataFrameWithDoubles, arrayDataFrame)
  }

  test("DataFrame contains vector") {
    val assembler = new VectorAssembler()
      .setInputCols(Array("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"))
      .setOutputCol("features")

    val vectorDataFrame = assembler.transform(prostateDataFrameWithDoubles).select("CAPSULE", "features")

    compareGbmOnTwoDatasets(prostateDataFrameWithDoubles, vectorDataFrame)
  }

  test("Testing dataset is missing one of feature columns") {
    val Array(trainingDF, rawTestingDF) = prostateDataFrame.randomSplit(Array(0.9, 0.1))
    val testingDF = rawTestingDF
      .drop("CAPSULE", "AGE") // Remove label and one of feature columns
      .cache()
    val gbm = configureGBMForProstateDF()

    val model = gbm.fit(trainingDF)
    val predictionDF = model.transform(testingDF)

    assertGBMPredictions(testingDF, predictionDF)
  }

  test("Testing dataset has an extra feature column") {
    val Array(trainingDF, rawTestingDF) = prostateDataFrame.randomSplit(Array(0.9, 0.1))
    val testingDF = rawTestingDF
      .drop("CAPSULE") // Remove label column
      .withColumn("EXTRA", rand()) // Add an extra column
      .cache()
    val gbm = configureGBMForProstateDF()

    val model = gbm.fit(trainingDF)
    val predictionDF = model.transform(testingDF)

    assertGBMPredictions(testingDF, predictionDF)
  }

  private def configureGBMForProstateDF(): H2OGBM = {
    new H2OGBM()
      .setSeed(42)
      .setDistribution("bernoulli")
      .setLabelCol("CAPSULE")
  }

  private def assertGBMPredictions(originalDF: DataFrame, predictionDF: DataFrame): Unit = {
    val records = predictionDF.select("detailed_prediction.probabilities").collect()
    val expectedNumberOfRecords = originalDF.count()
    records should have size expectedNumberOfRecords
    records.foreach { row =>
      val probabilities = row.getStruct(0)
      assert(probabilities.length == 2)
      probabilities.getDouble(0) should (be >= 0.0 and be <= 1.0)
      probabilities.getDouble(1) should (be >= 0.0 and be <= 1.0)
    }
  }

  private def testModelReload(name: String, df: DataFrame, model: H2OMOJOModel): Unit = {
    val predBeforeSave = model.transform(df)
    val modelFolder = tempFolder(name)
    model.write.overwrite.save(modelFolder)
    val reloadedModel = H2OMOJOModel.load(modelFolder)
    val predAfterReload = reloadedModel.transform(df)

    TestUtils.assertDataFramesAreIdentical(predBeforeSave, predAfterReload)
  }

  private def assertEqual(m1: H2OMOJOModel, m2: H2OMOJOModel, df: DataFrame): Unit = {
    val predMojo = m1.transform(df)
    val predModel = m2.transform(df)

    TestUtils.assertDataFramesAreIdentical(predMojo, predModel)
  }

  private def tempFolder(prefix: String) = {
    val path = java.nio.file.Files.createTempDirectory(prefix)
    path.toFile.deleteOnExit()
    path.toString
  }

  private lazy val irisDataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv("examples/smalldata/iris/iris_wheader.csv")
  }

  private lazy val prostateDataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("examples/smalldata/prostate/prostate.csv")
  }

  private def binomialModelFixture() = {
    val inputDf = prostateDataFrame
    val gbm = new H2OGBM()
      .setNtrees(2)
      .setSeed(42)
      .setDistribution("bernoulli")
      .setLabelCol("capsule")

    (inputDf, gbm.fit(inputDf))
  }

  private def multinomialModelFixture() = {
    val inputDf = irisDataFrame
    val gbm = new H2OGBM()
      .setNtrees(2)
      .setSeed(42)
      .setDistribution("multinomial")
      .setLabelCol("class")

    (inputDf, gbm.fit(inputDf))
  }

  private def regressionModelFixture() = {
    val inputDf = prostateDataFrame
    val gbm = new H2OGBM()
      .setNtrees(2)
      .setSeed(42)
      .setLabelCol("capsule")

    (inputDf, gbm.fit(inputDf))
  }

  private def deepLearningModelFixture() = {
    val inputDf = prostateDataFrame
    val dl = new H2ODeepLearning()
      .setSeed(42)
      .setReproducible(true)
      .setLabelCol("CAPSULE")

    (inputDf, dl.fit(inputDf))
  }

  private def savedBinomialModel() = {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("binom_model_prostate.mojo"),
      "binom_model_prostate.mojo")
    (prostateDataFrame, mojo)
  }

  private def savedRegressionModel() = {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("regre_model_prostate.mojo"),
      "regre_model_prostate.mojo")
    (prostateDataFrame, mojo)
  }

  private def savedMultinomialModel() = {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("multi_model_iris.mojo"),
      "multi_model_iris.mojo")
    (irisDataFrame, mojo)
  }

  private def savedDeepLearningModel() = {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("deep_learning_prostate.mojo"),
      "deep_learning_prostate.mojo")
    (prostateDataFrame, mojo)
  }

  test("getCurrentMetrics when trained with just training frame") {
    val estimator = new H2OGLM()
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = estimator.fit(prostateDataFrame)

    assert(model.getTrainingMetrics().nonEmpty)
    assert(model.getValidationMetrics().isEmpty)
    assert(model.getCrossValidationMetrics().isEmpty)
    assert(model.getCurrentMetrics().nonEmpty)
    assert(model.getTrainingMetrics() == model.getCurrentMetrics())
  }

  test("getCurrentMetrics when trained with validation frame") {
    val estimator = new H2OGLM()
      .setSeed(1)
      .setSplitRatio(0.8)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = estimator.fit(prostateDataFrame)

    assert(model.getTrainingMetrics().nonEmpty)
    assert(model.getValidationMetrics().nonEmpty)
    assert(model.getCrossValidationMetrics().isEmpty)
    assert(model.getCurrentMetrics().nonEmpty)
    assert(model.getValidationMetrics() == model.getCurrentMetrics())
  }

  test("getCurrentMetrics when trained with cross-validation") {
    val estimator = new H2OGLM()
      .setSeed(1)
      .setNfolds(3)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = estimator.fit(prostateDataFrame)

    assert(model.getTrainingMetrics().nonEmpty)
    assert(model.getValidationMetrics().isEmpty)
    assert(model.getCrossValidationMetrics().nonEmpty)
    assert(model.getCurrentMetrics().nonEmpty)
    assert(model.getCrossValidationMetrics() == model.getCurrentMetrics())
  }

  test("getCurrentMetrics when trained with validation frame and cross-validation") {
    val estimator = new H2OGLM()
      .setSeed(1)
      .setNfolds(3)
      .setSplitRatio(0.8)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = estimator.fit(prostateDataFrame)

    assert(model.getTrainingMetrics().nonEmpty)
    assert(model.getValidationMetrics().nonEmpty)
    assert(model.getCrossValidationMetrics().nonEmpty)
    assert(model.getCurrentMetrics().nonEmpty)
    assert(model.getCrossValidationMetrics() == model.getCurrentMetrics())
  }

  test("Exposed hex mojo model gives the same prediction as SW model") {
    val gbm = configureGBMForProstateDF()
    System.setProperty("spark.testing", "false")
    val model = gbm.fit(prostateDataFrame)
    val h2o3model = model.unwrapMojoModel()
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(h2o3model)
    val wrapper = new EasyPredictModelWrapper(config)

    val rowWithPrediction = model.transform(prostateDataFrame).first()
    val rowData = new RowData()
    rowData.put("ID", rowWithPrediction.get(0).toString())
    rowData.put("AGE", rowWithPrediction.get(2).toString())
    rowData.put("RACE", rowWithPrediction.get(3).toString())
    rowData.put("DPROS", rowWithPrediction.get(4).toString())
    rowData.put("DCAPS", rowWithPrediction.get(5).toString())
    rowData.put("PSA", rowWithPrediction.get(6).toString())
    rowData.put("VOL", rowWithPrediction.get(7).toString())
    rowData.put("GLEASON", rowWithPrediction.get(8).toString())

    val prediction = wrapper.predictBinomial(rowData)
    prediction.classProbabilities(0) shouldEqual rowWithPrediction.getStruct(9).getStruct(1).get(0)
    prediction.classProbabilities(1) shouldEqual rowWithPrediction.getStruct(9).getStruct(1).get(1)
  }

  test("getCrossValidationMetricsSummary returns a non-empty data frame when cross validation enabled") {
    val estimator = new H2OGLM()
      .setSeed(1)
      .setNfolds(3)
      .setSplitRatio(0.8)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = estimator.fit(prostateDataFrame)

    val crossValidationModelSummary = model.getCrossValidationMetricsSummary()

    val expectedColumns = Seq("metric", "mean", "sd", "cv_1_valid", "cv_2_valid", "cv_3_valid")
    crossValidationModelSummary.columns.toList shouldEqual expectedColumns
    crossValidationModelSummary.count() shouldBe >(0L)

    val row = crossValidationModelSummary.first()
    for (columnId <- 1 to 5) {
      row.getFloat(columnId) shouldBe >(0.0f)
    }
  }

  test("getStartType is exposed for loaded model") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("multi_model_iris.mojo"),
      "multi_model_iris.mojo")

    mojo.getStartTime() shouldBe 1631392711317L
  }

  test("getStartType is exposed for freshly trained model") {
    val (_, mojo) = multinomialModelFixture()

    mojo.getStartTime() should not be 0L
  }

  test("getEndTime is exposed for loaded model") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("multi_model_iris.mojo"),
      "multi_model_iris.mojo")

    mojo.getEndTime() shouldBe 1631392711360L
  }

  test("getEndTime is exposed for trained model") {
    val (_, mojo) = multinomialModelFixture()

    mojo.getEndTime() should not be 0L
  }

  test("getRunTime is exposed for loaded model") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("multi_model_iris.mojo"),
      "multi_model_iris.mojo")

    mojo.getRunTime() shouldBe 43L
  }

  test("getRunTime is exposed for trained model") {
    val (_, mojo) = multinomialModelFixture()

    mojo.getRunTime() should not be 0L
  }

  test("getDefaultThreshold is exposed for loaded model") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("binom_model_prostate.mojo"),
      "binom_model_prostate.mojo")

    mojo.getDefaultThreshold() shouldBe 0.40858428648438255
  }

  test("getDefaultThreshold returns default value if model doesn't contain the value") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("deep_learning_prostate.mojo"),
      "deep_learning_prostate.mojo")

    mojo.getDefaultThreshold() shouldBe 0.0
  }

  test("getDefaultThreshold is exposed for trained model") {
    val (_, mojo) = multinomialModelFixture()

    mojo.getDefaultThreshold() shouldBe 0.5
  }

  test("getCrossValidationScoringHistory returns histories when they are available") {
    val mojo =
      H2OMOJOModel.createFromMojo(this.getClass.getClassLoader.getResourceAsStream("gbm_cv.mojo"), "gbm_cv.mojo")

    val crossValidationModelsScoringHistory = mojo.getCrossValidationModelsScoringHistory()
    crossValidationModelsScoringHistory.length shouldBe 3

    for (historyDF <- crossValidationModelsScoringHistory) {
      historyDF.columns.length shouldBe 16
      historyDF.count() shouldBe 3L
    }
  }

  test("Cross validation models scoring history should be maintained when saving and loading model") {
    val mojo =
      H2OMOJOModel.createFromMojo(this.getClass.getClassLoader.getResourceAsStream("gbm_cv.mojo"), "gbm_cv.mojo")

    val name = "cv_scoring_history_reload.mojo"
    val modelFolder = tempFolder(name)
    mojo.write.overwrite.save(modelFolder)
    val reloadedModel = H2OMOJOModel.load(modelFolder)

    reloadedModel.getCrossValidationModelsScoringHistory().length shouldBe 3
    for (i <- 0 until mojo.getCrossValidationModelsScoringHistory().length) {
      TestUtils.assertDataFramesAreIdentical(
        mojo.getCrossValidationModelsScoringHistory()(i),
        reloadedModel.getCrossValidationModelsScoringHistory()(i))
    }
  }

  test("getCrossValidationModelsScoringHistory returns empty array when model doesn't contain it") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("deep_learning_prostate.mojo"),
      "deep_learning_prostate.mojo")

    mojo.getCrossValidationModelsScoringHistory().length shouldBe 0
  }

  {
    def numberOfFolds = 3

    lazy val gbm = configureGBMForProstateDF()
      .setNfolds(numberOfFolds)
      .setKeepCrossValidationModels(true)

    def trainedModel = gbm.fit(prostateDataFrame)

    lazy val model = {
      val path = "ml/build/model_with_cv_models"
      trainedModel.write.overwrite().save(path)
      H2OGBMMOJOModel.load(path)
    }

    test("Cross validation models are able to score") {
      val cvModels = model.getCrossValidationModels()
      cvModels.length shouldEqual numberOfFolds

      val result = model.transform(prostateDataFrame)
      val cvResult = cvModels(0).transform(prostateDataFrame)

      result.schema shouldEqual cvResult.schema
      result.count() shouldEqual cvResult.count()
      cvResult.show(truncate = false)
    }

    test("Cross validation models can provide training and validation metrics") {
      val cvModels = model.getCrossValidationModels()
      cvModels.length shouldEqual numberOfFolds
      val firstCVModel = cvModels(0)

      firstCVModel.getTrainingMetrics() should not be (null)
      firstCVModel.getTrainingMetrics() should not be (Map.empty)
      firstCVModel.getValidationMetrics() should not be (null)
      firstCVModel.getValidationMetrics() should not be (Map.empty)
      firstCVModel.getCrossValidationMetrics() should be(Map.empty)

      firstCVModel.getTrainingMetrics().keySet shouldEqual model.getTrainingMetrics().keySet
      for ((k, v) <- firstCVModel.getTrainingMetrics()) {
        assert(v != Double.NaN && v != 0, s"The training metric $k has value '$v'")
      }

      firstCVModel.getValidationMetrics().keySet shouldEqual model.getCrossValidationMetrics().keySet
      for ((k, v) <- firstCVModel.getValidationMetrics()) {
        assert(v != Double.NaN && v != 0, s"The validation metric $k has value '$v'")
      }
    }

    test("Cross validation models can provide scoring history") {
      val cvModels = model.getCrossValidationModels()
      cvModels.length shouldEqual numberOfFolds
      val cvScoringHistory = cvModels(0).getScoringHistory()

      cvScoringHistory.columns shouldEqual Array(
        "Timestamp",
        "Duration",
        "Number of Trees",
        "Training RMSE",
        "Training LogLoss",
        "Training AUC",
        "Training pr_auc",
        "Training Lift",
        "Training Classification Error",
        "Validation RMSE",
        "Validation LogLoss",
        "Validation AUC",
        "Validation pr_auc",
        "Validation Lift",
        "Validation Classification Error")
      cvScoringHistory.count() shouldBe >(0L)
    }

    test("Cross validation models can provide feature importances") {
      val cvModels = model.getCrossValidationModels()
      cvModels.length shouldEqual numberOfFolds
      val featureImportances = cvModels(0).getFeatureImportances()

      featureImportances.columns shouldEqual Array("Variable", "Relative Importance", "Scaled Importance", "Percentage")
      featureImportances.count() shouldBe >(0L)
      featureImportances.count() shouldEqual gbm.getFeaturesCols().length
    }

    test("Cross validation models are null when simple validation is used") {
      val gbm = configureGBMForProstateDF().setSplitRatio(0.8)
      val model = gbm.fit(prostateDataFrame)

      val cvModels = model.getCrossValidationModels()

      cvModels should be(null)
    }

    test("Cross validation models are null if generating of them is disabled") {
      val gbm = configureGBMForProstateDF().setKeepCrossValidationModels(true)
      val model = gbm.fit(prostateDataFrame)

      val cvModels = model.getCrossValidationModels()

      cvModels should be(null)
    }
  }

  test("H2ODeepLearningMOJOModel should return model summary") {
    val (_, mojoModel) = savedDeepLearningModel()

    val summary = mojoModel.getModelSummary()
    val summaryCollected = summary.collect()

    summaryCollected should have size 4
    val expectedFields = Seq(
      "Layer",
      "Units",
      "Type",
      "Dropout",
      "L1",
      "L2",
      "Mean Rate",
      "Rate RMS",
      "Momentum",
      "Mean Weight",
      "Weight RMS",
      "Mean Bias",
      "Bias RMS")
    summary.schema.fieldNames should contain theSameElementsAs expectedFields
    summaryCollected.map(_.getValuesMap(expectedFields)) should contain theSameElementsAs
      Seq(
        Map(
          "Layer" -> 1,
          "Units" -> 8,
          "L1" -> null,
          "Bias RMS" -> null,
          "Mean Bias" -> null,
          "Mean Weight" -> null,
          "Momentum" -> null,
          "Rate RMS" -> null,
          "Mean Rate" -> null,
          "Dropout" -> 0.0,
          "L2" -> null,
          "Weight RMS" -> null,
          "Type" -> "Input"),
        Map(
          "Layer" -> 2,
          "Units" -> 200,
          "L1" -> 0.0,
          "Bias RMS" -> 0.049144044518470764,
          "Mean Bias" -> 0.42625558799512825,
          "Mean Weight" -> 0.0020895117304439736,
          "Momentum" -> 0.0,
          "Rate RMS" -> 0.0030197836458683014,
          "Mean Rate" -> 0.006225864375919627,
          "Dropout" -> 0.0,
          "L2" -> 0.0,
          "Weight RMS" -> 0.09643048048019409,
          "Type" -> "Rectifier"),
        Map(
          "Layer" -> 3,
          "Units" -> 200,
          "L1" -> 0.0,
          "Bias RMS" -> 0.008990883827209473,
          "Mean Bias" -> 0.9844640783479953,
          "Mean Weight" -> -0.008243563556700311,
          "Momentum" -> 0.0,
          "Rate RMS" -> 0.09206506609916687,
          "Mean Rate" -> 0.04241905607206281,
          "Dropout" -> 0.0,
          "L2" -> 0.0,
          "Weight RMS" -> 0.06984925270080566,
          "Type" -> "Rectifier"),
        Map(
          "Layer" -> 4,
          "Units" -> 1,
          "L1" -> 0.0,
          "Bias RMS" -> 1.0971281125650402e-154,
          "Mean Bias" -> 0.002604305485232783,
          "Mean Weight" -> 9.763148391539289e-4,
          "Momentum" -> 0.0,
          "Rate RMS" -> 9.573120623826981e-4,
          "Mean Rate" -> 6.254940157668898e-4,
          "Dropout" -> null,
          "L2" -> 0.0,
          "Weight RMS" -> 0.06601589918136597,
          "Type" -> "Linear"))
  }

  test("H2OGBMMOJOModel should return model summary") {
    val (_, mojoModel) = savedBinomialModel()

    val summary = mojoModel.getModelSummary()
    val summaryCollected = summary.collect()

    summaryCollected should have size 1
    val expectedFields = Array(
      "Number of Trees",
      "Number of Internal Trees",
      "Model Size in Bytes",
      "Min. Depth",
      "Max. Depth",
      "Mean Depth",
      "Min. Leaves",
      "Max. Leaves",
      "Mean Leaves")
    summary.schema.fieldNames should contain theSameElementsAs expectedFields
    summaryCollected.map(_.getValuesMap(expectedFields)).head should contain theSameElementsAs
      Map(
        "Min. Depth" -> 5,
        "Max. Leaves" -> 24,
        "Number of Internal Trees" -> 2,
        "Min. Leaves" -> 22,
        "Mean Depth" -> 5.0,
        "Number of Trees" -> 2,
        "Model Size in Bytes" -> 694,
        "Max. Depth" -> 5,
        "Mean Leaves" -> 23.0)
  }

  test("should return formatted model description with all the deep learning layers included") {
    val (_, mojoModel) = savedDeepLearningModel()

    val mojoStringSummary = mojoModel.toString

    mojoStringSummary should startWith("""Model Details
      |===============
      |H2ODeepLearning
      |Model Key: deep_learning_prostate.mojo""".stripMargin)

    mojoStringSummary should include("""Model summary
      |Layer: 1
      |Units: 8
      |Type: Input
      |Dropout: 0.0
      |L1: null
      |L2: null
      |Mean Rate: null
      |Rate RMS: null
      |Momentum: null
      |Mean Weight: null
      |Weight RMS: null
      |Mean Bias: null
      |Bias RMS: null
      |
      |Layer: 2
      |Units: 200
      |Type: Rectifier
      |Dropout: 0.0
      |L1: 0.0
      |L2: 0.0
      |Mean Rate: 0.006225864375919627
      |Rate RMS: 0.0030197836458683014
      |Momentum: 0.0
      |Mean Weight: 0.0020895117304439736
      |Weight RMS: 0.09643048048019409
      |Mean Bias: 0.42625558799512825
      |Bias RMS: 0.049144044518470764
      |
      |Layer: 3
      |Units: 200
      |Type: Rectifier
      |Dropout: 0.0
      |L1: 0.0
      |L2: 0.0
      |Mean Rate: 0.04241905607206281
      |Rate RMS: 0.09206506609916687
      |Momentum: 0.0
      |Mean Weight: -0.008243563556700311
      |Weight RMS: 0.06984925270080566
      |Mean Bias: 0.9844640783479953
      |Bias RMS: 0.008990883827209473
      |
      |Layer: 4
      |Units: 1
      |Type: Linear
      |Dropout: null
      |L1: 0.0
      |L2: 0.0
      |Mean Rate: 6.254940157668898E-4
      |Rate RMS: 9.573120623826981E-4
      |Momentum: 0.0
      |Mean Weight: 9.763148391539289E-4
      |Weight RMS: 0.06601589918136597
      |Mean Bias: 0.002604305485232783
      |Bias RMS: 1.0971281125650402E-154""".stripMargin)

    mojoStringSummary should include("""Training metrics
      |RMSLE: 0.28540432947505806
      |Nobs: 380.0
      |RMSE: 0.3988910324498237
      |MAE: 0.3341350756077194
      |MeanResidualDeviance: 0.1591140557688863
      |ScoringTime: 1.513802166234E12
      |MSE: 0.1591140557688863
      |R2: 0.33845643220675536""".stripMargin)

    mojoStringSummary should endWith(
      """More info available using methods like:
        |getFeatureImportances(), getScoringHistory(), getCrossValidationScoringHistory()""".stripMargin)
  }

  test("should return standard formatted model description with available metrics") {
    val mojoModel =
      H2OMOJOModel.createFromMojo(this.getClass.getClassLoader.getResourceAsStream("gbm_cv.mojo"), "gbm_cv.mojo")

    val mojoStringSummary = mojoModel.toString
    mojoStringSummary should startWith("""Model Details
      |===============
      |H2OGBM
      |Model Key: gbm_cv.mojo""".stripMargin)

    mojoStringSummary should include("""Model summary
      |Number of Trees: 2
      |Number of Internal Trees: 2
      |Model Size in Bytes: 694
      |Min. Depth: 5
      |Max. Depth: 5
      |Mean Depth: 5.0
      |Min. Leaves: 22
      |Max. Leaves: 24
      |Mean Leaves: 23.0""".stripMargin)

    mojoStringSummary should include("""Training metrics
      |PRAUC: 0.8638163431669145
      |Nobs: 380.0
      |Logloss: 0.5972755287156798
      |Gini: 0.7937577380438225
      |RMSE: 0.4511854642769045
      |ScoringTime: 1.628871831383E12
      |MSE: 0.20356832317476586
      |R2: 0.15363030530545652
      |MeanPerClassError: 0.1926089084679393
      |AUC: 0.8968788690219113""".stripMargin)

    mojoStringSummary should include("""Cross validation metrics
      |PRAUC: 0.6255892513115805
      |Nobs: 380.0
      |Logloss: 0.6365971538694172
      |Gini: 0.42688088451239525
      |RMSE: 0.4717010343761202
      |ScoringTime: 1.628871831395E12
      |MSE: 0.22250186583150172
      |R2: 0.07491090305292536
      |MeanPerClassError: 0.32341424087990556
      |AUC: 0.7134404422561976""".stripMargin)

    mojoStringSummary should endWith(
      """More info available using methods like:
      |getFeatureImportances(), getScoringHistory(), getCrossValidationScoringHistory()""".stripMargin)
  }

}
