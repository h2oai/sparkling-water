package org.apache.spark.ml.h2o.features

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.h2o.algos.H2OGBM
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OTargetEncoderTest extends FunSuite with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", "H2OTargetEncoderTest", conf = defaultSparkConf)

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

  private def assertDataFramesAreIdentical(expected: DataFrame, produced: DataFrame): Unit = {
    produced.cache()
    val numberOfExtraColumnsInExpected = expected.exceptAll(produced).count()
    val numberOfExtraColumnsInProduced = produced.exceptAll(expected).count()
    produced.unpersist()
    assert(
      numberOfExtraColumnsInExpected == 0 && numberOfExtraColumnsInProduced == 0,
      s"""The expected data frame contains $numberOfExtraColumnsInExpected rows that are not in the produced data frame.
         |The produced data frame contains $numberOfExtraColumnsInProduced rows that are not in the expected data frame.
       """.stripMargin)
  }

  private lazy val dataset = loadDataFrameFromCsv("smalldata/prostate/prostate.csv")
  private lazy val Array(trainingDataset, testingDataset) = dataset.randomSplit(Array(0.8, 0.2), 1234L).map(_.cache())
  private lazy val expectedTestingDataset = loadDataFrameFromCsvAsResource("/target_encoder/testing_dataset_transformed.csv").cache()

  test("The pipeline with a target encoder transform training and testing dataset without an exception") {
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
    val transformedTestingDataset = model.transform(testingDataset)

    assertDataFramesAreIdentical(expectedTestingDataset, transformedTestingDataset)
  }

  test("The target encoder doesn't apply noise on the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setNoise(H2OTargetEncoderNoiseSettings(amount = 0.5))
    val pipeline = new Pipeline().setStages(Array(targetEncoder))

    val model = pipeline.fit(trainingDataset)
    val transformedTestingDataset = model.transform(testingDataset)

    assertDataFramesAreIdentical(expectedTestingDataset, transformedTestingDataset)
  }
}
