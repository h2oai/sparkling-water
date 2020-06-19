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
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OTargetEncoderTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private def loadDataFrameFromCsv(path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TestUtils.locate(path))
  }

  private def loadDataFrameFromCsvAsResource(path: String): DataFrame = {
    val filePath = getClass.getResource(path).getFile
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
  }

  private lazy val dataset = loadDataFrameFromCsv("smalldata/prostate/prostate.csv")
  private lazy val trainingDataset = dataset.limit(300).cache()
  private lazy val testingDataset = dataset.except(trainingDataset).cache()
  private lazy val expectedTestingDataset =
    loadDataFrameFromCsvAsResource("/target_encoder/testing_dataset_transformed.csv").cache()

  test("A pipeline with a target encoder transform training and testing dataset without an exception") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
    val gbm = new H2OGBM().setLabelCol("CAPSULE")

    val pipeline = new Pipeline().setStages(Array(targetEncoder, gbm))

    val model = pipeline.fit(trainingDataset)
    model.transform(testingDataset)
  }

  test("The target encoder is able to transform dataset after it's saved and loaded") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
    val pipeline = new Pipeline().setStages(Array(targetEncoder))

    val model = pipeline.fit(trainingDataset)
    val path = "build/ml/targetEncoder_save_load"
    model.write.overwrite().save(path)
    val loadedModel = PipelineModel.load(path)
    val transformedTestingDataset = loadedModel.transform(testingDataset)

    TestUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedTestingDataset)
  }

  test("The target encoder doesn't apply noise on the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setNoise(0.5)
    val pipeline = new Pipeline().setStages(Array(targetEncoder))

    val model = pipeline.fit(trainingDataset)
    val transformedTestingDataset = model.transform(testingDataset)

    TestUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedTestingDataset)
  }

  test("TargetEncoderModel with disabled noise and TargetEncoderMOJOModel transform the training dataset the same way") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)
    val targetEncoderModel = targetEncoder.fit(trainingDataset)

    val transformedByModel = targetEncoderModel.transformTrainingDataset(trainingDataset)
    val transformedByMOJOModel = targetEncoderModel.transform(trainingDataset)

    TestUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test("TargetEncoderModel with disabled noise and TargetEncoderMOJOModel apply blended average the same way") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setBlendedAvgEnabled(true)
      .setNoise(0.0)
    val targetEncoderModel = targetEncoder.fit(trainingDataset)

    val transformedByModel = targetEncoderModel.transformTrainingDataset(trainingDataset)
    val transformedByMOJOModel = targetEncoderModel.transform(trainingDataset)

    TestUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test("TargetEncoderMOJOModel will use global average for unexpected values in the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DCAPS"))
      .setLabelCol("CAPSULE")

    val unexpectedValuesDF = testingDataset.withColumn("DCAPS", lit(10))
    val expectedValue = trainingDataset.groupBy().avg("CAPSULE").collect()(0).getDouble(0)
    val expectedDF = unexpectedValuesDF.withColumn("DCAPS_te", lit(expectedValue))
    val model = targetEncoder.fit(trainingDataset)

    val resultDF = model.transform(unexpectedValuesDF)

    TestUtils.assertDataFramesAreIdentical(expectedDF, resultDF)
  }

  test("TargetEncoderModel will use global average for unexpected values in the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DCAPS"))
      .setLabelCol("CAPSULE")

    val unexpectedValuesDF = testingDataset.withColumn("DCAPS", lit(10))
    val expectedValue = trainingDataset.groupBy().avg("CAPSULE").collect()(0).getDouble(0)
    val expectedDF = unexpectedValuesDF.withColumn("DCAPS_te", lit(expectedValue))
    val model = targetEncoder.fit(trainingDataset)

    val resultDF = model.transformTrainingDataset(unexpectedValuesDF)

    TestUtils.assertDataFramesAreIdentical(expectedDF, resultDF)
  }

  test("TargetEncoderMOJOModel will use global average for null values in the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DCAPS"))
      .setLabelCol("CAPSULE")

    val withNullsDF = testingDataset.withColumn("DCAPS", lit(null).cast(IntegerType))
    val expectedValue = trainingDataset.groupBy().avg("CAPSULE").collect()(0).getDouble(0)
    val expectedDF = withNullsDF.withColumn("DCAPS_te", lit(expectedValue))
    val model = targetEncoder.fit(trainingDataset)

    val resultDF = model.transform(withNullsDF)

    TestUtils.assertDataFramesAreIdentical(expectedDF, resultDF)
  }

  test("TargetEncoderModel will use global average for null values in the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DCAPS"))
      .setLabelCol("CAPSULE")

    val withNullsDF = testingDataset.withColumn("DCAPS", lit(null).cast(IntegerType))
    val expectedValue = trainingDataset.groupBy().avg("CAPSULE").collect()(0).getDouble(0)
    val expectedDF = withNullsDF.withColumn("DCAPS_te", lit(expectedValue))
    val model = targetEncoder.fit(trainingDataset)

    val resultDF = model.transformTrainingDataset(withNullsDF)

    TestUtils.assertDataFramesAreIdentical(expectedDF, resultDF)
  }

  test("The targetEncoder can be trained and used on a dataset with null values") {
    import spark.implicits._
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)

    val trainingWithNullsDF = trainingDataset
      .withColumn("DCAPS", when(rand(1) < 0.5, 'DCAPS).otherwise(lit(null)))
      .cache()

    val model = targetEncoder.fit(trainingWithNullsDF)
    val transformedByModel = model.transformTrainingDataset(trainingWithNullsDF).cache()
    val transformedByMOJOModel = model.transform(trainingWithNullsDF).cache()

    transformedByModel.filter('DCAPS_te.isNull).count() shouldBe 0
    transformedByMOJOModel.filter('DCAPS_te.isNull).count() shouldBe 0

    TestUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test("The KFold strategy with column should give the same results as LeaveOneOut on the training dataset") {
    val targetEncoderKFold = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("KFold")
      .setFoldCol("ID")
      .setNoise(0.0)

    val targetEncoderLeaveOneOut = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("LeaveOneOut")
      .setNoise(0.0)

    val modelKFold = targetEncoderKFold.fit(trainingDataset)
    val modelLeaveOneOut = targetEncoderLeaveOneOut.fit(trainingDataset)
    val transformedKFold = modelKFold.transformTrainingDataset(trainingDataset)
    val transformedLeaveOneOut = modelLeaveOneOut.transformTrainingDataset(trainingDataset)

    TestUtils.assertDataFramesAreIdentical(transformedLeaveOneOut, transformedKFold)
  }

  test("The target encoder treats string columns as other types") {
    import spark.implicits._

    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)
    val datasetWithStrings = dataset
      .withColumn("RACE", 'RACE cast StringType)
      .withColumn("DPROS", 'DPROS cast StringType)
      .withColumn("DCAPS", 'DCAPS cast StringType)
      .withColumn("CAPSULE", 'CAPSULE cast StringType)
    val trainingDataset = dataset.limit(300).cache()
    val testingDataset = dataset.except(trainingDataset).cache()
    val model = targetEncoder.fit(trainingDataset)

    val transformedByModel = model.transformTrainingDataset(testingDataset)
    val transformedByMOJOModel = model.transform(testingDataset)

    TestUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedByModel)
    TestUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedByMOJOModel)
  }

  test("The target encoder can work with arbitrary label categories") {
    val trainingDatasetWithLabel =
      trainingDataset.withColumn("LABEL", when(rand(1) < 0.5, lit("a")).otherwise(lit("b")))
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("LABEL")
      .setHoldoutStrategy("None")
      .setNoise(0.0)
    val model = targetEncoder.fit(trainingDatasetWithLabel)

    val transformedByModel = model.transformTrainingDataset(trainingDatasetWithLabel)
    val transformedByMOJOModel = model.transform(trainingDatasetWithLabel)

    TestUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test("The fit function throws a runtime exception when the label domain has more than two categories") {
    val trainingDatasetWithLabel = trainingDataset.withColumn(
      "LABEL",
      when(rand(1) < 0.3, lit("a"))
        .when(rand(1) < 0.5, lit("b"))
        .otherwise(lit("c")))
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("LABEL")
      .setHoldoutStrategy("None")
      .setNoise(0.0)

    val thrown = intercept[RuntimeException] {
      targetEncoder.fit(trainingDatasetWithLabel)
    }
    assert(thrown.getMessage == "The label column can not contain more than two unique values.")
  }

  test(
    "TargetEncoderModel with disabled noise and TargetEncoderMOJOModel transform a dataset with an unexpected label the same way") {
    val trainingDatasetWithLabel =
      trainingDataset.withColumn("LABEL", when(rand(1) < 0.5, lit("a")).otherwise(lit("b")))
    val testingDatasetWithLabel = testingDataset.withColumn("LABEL", lit("c"))
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("LABEL")
      .setHoldoutStrategy("None")
      .setNoise(0.0)
    val model = targetEncoder.fit(trainingDatasetWithLabel)

    val transformedByModel = model.transformTrainingDataset(testingDatasetWithLabel)
    val transformedByMOJOModel = model.transform(testingDatasetWithLabel)

    TestUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test(
    "TargetEncoderModel with disabled noise and TargetEncoderMOJOModel transform a dataset with a null label the same way") {
    val trainingDatasetWithLabel =
      trainingDataset.withColumn("LABEL", when(rand(1) < 0.5, lit("a")).otherwise(lit("b")))
    val testingDatasetWithLabel = testingDataset.withColumn("LABEL", when(rand(1) < 0.5, lit("a")).otherwise(lit(null)))
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("LABEL")
      .setHoldoutStrategy("None")
      .setNoise(0.0)
    val model = targetEncoder.fit(trainingDatasetWithLabel)

    val transformedByModel = model.transformTrainingDataset(testingDatasetWithLabel)
    val transformedByMOJOModel = model.transform(testingDatasetWithLabel)

    TestUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test("TargetEncoderModel returns the same result regardless the order of the inputCols specification") {
    import spark.implicits._

    def transformTrainingDataset(inputCols: Array[String]): DataFrame = {
      val targetEncoder = new H2OTargetEncoder()
        .setInputCols(inputCols)
        .setLabelCol("CAPSULE")
        .setHoldoutStrategy("None")
        .setNoise(0.0)

      val model = targetEncoder.fit(trainingDataset)
      model.transformTrainingDataset(trainingDataset)
    }

    val expectedResult = transformTrainingDataset(Array("RACE", "DPROS", "DCAPS"))
    val result = transformTrainingDataset(Array("DPROS", "DCAPS", "RACE"))
      .select('ID, 'CAPSULE, 'AGE, 'RACE, 'DPROS, 'DCAPS, 'PSA, 'VOL, 'GLEASON, 'RACE_te, 'DPROS_te, 'DCAPS_te)

    TestUtils.assertDataFramesAreIdentical(expectedResult, result)
  }

  test("TargetEncoderMOJOModel returns the same result regardless the order of the inputCols specification") {
    import spark.implicits._

    def transformTestingDataset(inputCols: Array[String]): DataFrame = {
      val targetEncoder = new H2OTargetEncoder()
        .setInputCols(inputCols)
        .setLabelCol("CAPSULE")
        .setHoldoutStrategy("None")
        .setNoise(0.0)

      val model = targetEncoder.fit(trainingDataset)
      model.transform(testingDataset)
    }

    val expectedResult = transformTestingDataset(Array("RACE", "DPROS", "DCAPS"))
    val result = transformTestingDataset(Array("DPROS", "DCAPS", "RACE"))
      .select('ID, 'CAPSULE, 'AGE, 'RACE, 'DPROS, 'DCAPS, 'PSA, 'VOL, 'GLEASON, 'RACE_te, 'DPROS_te, 'DCAPS_te)

    TestUtils.assertDataFramesAreIdentical(expectedResult, result)
  }

  test("TargetEncoderModel transforms a dataset regardless the order of columns") {
    import spark.implicits._
    val originalOrder = Array($"ID", $"RACE", $"DPROS", $"DCAPS", $"CAPSULE")
    val trainingSubDataset = trainingDataset.select(originalOrder: _*)
    val testingSubDataset = testingDataset.select(originalOrder: _*)

    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)

    val model = targetEncoder.fit(trainingSubDataset)
    val expectedResult = model.transformTrainingDataset(testingSubDataset)

    val result = model
      .transformTrainingDataset(testingSubDataset.select(originalOrder.reverse: _*))
      .select(originalOrder ++ Array($"RACE_te", $"DPROS_te", $"DCAPS_te"): _*)

    TestUtils.assertDataFramesAreIdentical(expectedResult, result)
  }

  test("TargetEncoderMOJOModel transforms a dataset regardless the order of columns") {
    import spark.implicits._
    val originalOrder = Array($"ID", $"RACE", $"DPROS", $"DCAPS", $"CAPSULE")
    val trainingSubDataset = trainingDataset.select(originalOrder: _*)
    val testingSubDataset = testingDataset.select(originalOrder: _*)

    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)

    val model = targetEncoder.fit(trainingSubDataset)
    val expectedResult = model.transform(testingSubDataset)

    val result = model
      .transform(testingSubDataset.select(originalOrder.reverse: _*))
      .select(originalOrder ++ Array($"RACE_te", $"DPROS_te", $"DCAPS_te"): _*)

    TestUtils.assertDataFramesAreIdentical(expectedResult, result)
  }

  test("TargetEncoderModel transforms a dataset with a subset of columns the same way as full dataset") {
    import spark.implicits._
    val order = Array($"ID", $"RACE", $"DPROS", $"DCAPS", $"CAPSULE")
    val testingSubDataset = testingDataset.select(order: _*)

    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)

    val model = targetEncoder.fit(trainingDataset)
    val expectedResult = model
      .transformTrainingDataset(testingSubDataset)
      .select(order ++ Array($"RACE_te", $"DPROS_te", $"DCAPS_te"): _*)

    val result = model.transformTrainingDataset(testingSubDataset)

    TestUtils.assertDataFramesAreIdentical(expectedResult, result)
  }

  test("TargetEncoderMOJOModel transforms a dataset with a subset of columns the same way as full dataset") {
    import spark.implicits._
    val order = Array($"ID", $"RACE", $"DPROS", $"DCAPS", $"CAPSULE")
    val testingSubDataset = testingDataset.select(order: _*)

    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)

    val model = targetEncoder.fit(trainingDataset)
    val expectedResult = model
      .transform(testingDataset)
      .select(order ++ Array($"RACE_te", $"DPROS_te", $"DCAPS_te"): _*)

    val result = model.transform(testingSubDataset)

    TestUtils.assertDataFramesAreIdentical(expectedResult, result)
  }

  test("TargetEncoderModel supports custom outputCols") {
    import spark.implicits._

    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DPROS", "DCAPS", "RACE"))
      .setOutputCols(Array("DPROS_out", "DCAPS_out", "RACE_out"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)
    val expected = expectedTestingDataset
      .withColumn("DPROS_out", 'DPROS_te)
      .drop('DPROS_te)
      .withColumn("DCAPS_out", 'DCAPS_te)
      .drop('DCAPS_te)
      .withColumn("RACE_out", 'RACE_te)
      .drop('RACE_te)

    val model = targetEncoder.fit(trainingDataset)
    val transformedTestingDataset = model.transformTrainingDataset(testingDataset)

    TestUtils.assertDataFramesAreIdentical(expected, transformedTestingDataset)
  }

  test("TargetEncoderMOJOModel supports custom outputCols") {
    import spark.implicits._

    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DPROS", "DCAPS", "RACE"))
      .setOutputCols(Array("DPROS_out", "DCAPS_out", "RACE_out"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy("None")
      .setNoise(0.0)
    val expected = expectedTestingDataset
      .withColumn("DPROS_out", 'DPROS_te)
      .drop('DPROS_te)
      .withColumn("DCAPS_out", 'DCAPS_te)
      .drop('DCAPS_te)
      .withColumn("RACE_out", 'RACE_te)
      .drop('RACE_te)

    val model = targetEncoder.fit(trainingDataset)
    val transformedTestingDataset = model.transform(testingDataset)

    TestUtils.assertDataFramesAreIdentical(expected, transformedTestingDataset)
  }
}
