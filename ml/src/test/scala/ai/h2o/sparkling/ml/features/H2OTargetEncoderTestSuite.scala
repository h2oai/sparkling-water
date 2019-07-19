package ai.h2o.sparkling.ml.features

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.apache.spark.ml.h2o.algos.H2OGBM
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OTargetEncoderTestSuite extends FunSuite with SharedH2OTestContext {

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

    TestFrameUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedTestingDataset)
  }

  test("The target encoder doesn't apply noise on the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setNoise(0.5)
    val pipeline = new Pipeline().setStages(Array(targetEncoder))

    val model = pipeline.fit(trainingDataset)
    val transformedTestingDataset = model.transform(testingDataset)

    TestFrameUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedTestingDataset)
  }

  test("TargetEncoderModel with disabled noise and TargetEncoderMOJOModel transform the training dataset the same way") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy(H2OTargetEncoderHoldoutStrategy.None)
      .setNoise(0.0)
    val targetEncoderModel = targetEncoder.fit(trainingDataset)

    val transformedByModel = targetEncoderModel.transformTrainingDataset(trainingDataset)
    val transformedByMOJOModel = targetEncoderModel.transform(trainingDataset)

    TestFrameUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test("TargetEncoderModel with disabled noise and TargetEncoderMOJOModel apply blended average the same way") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy(H2OTargetEncoderHoldoutStrategy.None)
      .setBlendedAvgEnabled(true)
      .setNoise(0.0)
    val targetEncoderModel = targetEncoder.fit(trainingDataset)

    val transformedByModel = targetEncoderModel.transformTrainingDataset(trainingDataset).cache()
    val transformedByMOJOModel = targetEncoderModel.transform(trainingDataset).cache()

    TestFrameUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test("The target encoder will use global average for unexpected values in the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DCAPS"))
      .setLabelCol("CAPSULE")

    val unexpectedValuesDF = testingDataset.withColumn("DCAPS", lit(10))
    val expectedValue = trainingDataset.groupBy().avg("CAPSULE").collect()(0).getDouble(0)
    val expectedDF = unexpectedValuesDF.withColumn("DCAPS_te", lit(expectedValue))
    val model = targetEncoder.fit(trainingDataset)

    val resultDF = model.transform(unexpectedValuesDF)

    TestFrameUtils.assertDataFramesAreIdentical(expectedDF, resultDF)
  }

  test("The target encoder will use global average for null values in the testing dataset") {
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("DCAPS"))
      .setLabelCol("CAPSULE")

    val withNullsDF = testingDataset.withColumn("DCAPS", lit(null).cast(IntegerType))
    val expectedValue = trainingDataset.groupBy().avg("CAPSULE").collect()(0).getDouble(0)
    val expectedDF = withNullsDF.withColumn("DCAPS_te", lit(expectedValue))
    val model = targetEncoder.fit(trainingDataset)

    val resultDF = model.transform(withNullsDF)

    TestFrameUtils.assertDataFramesAreIdentical(expectedDF, resultDF)
  }

  test("The targetEncoder can be trained and used on a dataset with null values") {
    import spark.implicits._
    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy(H2OTargetEncoderHoldoutStrategy.None)
      .setNoise(0.0)

    val trainingWithNullsDF = trainingDataset
      .withColumn("DCAPS", when(rand(1) < 0.5, 'DCAPS).otherwise(lit(null)))
      .cache()

    val model = targetEncoder.fit(trainingWithNullsDF)
    val transformedByModel = model.transformTrainingDataset(trainingWithNullsDF).cache()
    val transformedByMOJOModel = model.transform(trainingWithNullsDF).cache()

    assert(transformedByModel.filter('DCAPS_te.isNull).count() == 0)
    assert(transformedByMOJOModel.filter('DCAPS_te.isNull).count() == 0)
    TestFrameUtils.assertDataFramesAreIdentical(transformedByModel, transformedByMOJOModel)
  }

  test("The KFold strategy with column should give the same results as LeaveOneOut on the training dataset") {
    val targetEncoderKFold = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy(H2OTargetEncoderHoldoutStrategy.KFold)
      .setFoldCol("ID")
      .setNoise(0.0)

    val targetEncoderLeaveOneOut = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy(H2OTargetEncoderHoldoutStrategy.LeaveOneOut)
      .setNoise(0.0)

    val modelKFold = targetEncoderKFold.fit(trainingDataset)
    val modelLeaveOneOut = targetEncoderLeaveOneOut.fit(trainingDataset)
    val transformedKFold = modelKFold.transformTrainingDataset(trainingDataset)
    val transformedLeaveOneOut = modelLeaveOneOut.transformTrainingDataset(trainingDataset)

    TestFrameUtils.assertDataFramesAreIdentical(transformedLeaveOneOut, transformedKFold)
  }

  test("The target encoder treats string columns as other types ") {
    import spark.implicits._

    val targetEncoder = new H2OTargetEncoder()
      .setInputCols(Array("RACE", "DPROS", "DCAPS"))
      .setLabelCol("CAPSULE")
      .setHoldoutStrategy(H2OTargetEncoderHoldoutStrategy.None)
      .setNoise(0.0)
    val datasetWithStrings = dataset
      .withColumn("RACE", 'RACE cast StringType)
      .withColumn("DPROS", 'DPROS cast StringType)
      .withColumn("DCAPS", 'DCAPS cast StringType)
      .withColumn("CAPSULE", 'CAPSULE cast StringType)
    val Array(trainingDataset, testingDateset) = datasetWithStrings.randomSplit(Array(0.8, 0.2), 1234L)
    val model = targetEncoder.fit(trainingDataset)

    val transformedByModel = model.transformTrainingDataset(testingDataset)
    val transformedByMOJOModel = model.transform(testingDateset)

    TestFrameUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedByModel)
    TestFrameUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedByMOJOModel)
  }
}
