package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.algos._
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import hex.Model
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import ai.h2o.sparkling.ml.ParameterSetters._
import org.apache.spark.sql.functions.lit

@RunWith(classOf[JUnitRunner])
class H2OSupervisedMOJOModelTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .cache()

  private lazy val Array(trainingDataset, testingDataset) = dataset.randomSplit(Array(0.8, 0.2), 1234L).map(_.cache())

  private def testOffsetColumnGetsPropagatedToMOJOModel(algo: H2OSupervisedAlgorithm[_ <: Model.Parameters]): Unit = {
    val offsetColumn = "PSA"
    algo
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
      .setOffsetCol(offsetColumn)

    val pipeline = new Pipeline().setStages(Array(algo))

    val model = pipeline.fit(dataset)
    val path = s"ml/build/offset_propagation_to_mojo_${algo.getClass.getSimpleName}"
    model.write.overwrite().save(path)
    val loadedModel = PipelineModel.load(path)

    def getModelOffset(model: PipelineModel): String = {
      model.stages(0).asInstanceOf[H2OSupervisedMOJOModel].getOffsetCol()
    }

    val modelOffset = getModelOffset(model)
    val deserializedModelOffset = getModelOffset(loadedModel)

    modelOffset shouldEqual offsetColumn
    deserializedModelOffset shouldEqual offsetColumn
  }

  test("Offset column gets propagated to MOJO model - GBM") {
    testOffsetColumnGetsPropagatedToMOJOModel(new H2OGBM())
  }

  test("Offset column gets propagated to MOJO model - XGBoost") {
    testOffsetColumnGetsPropagatedToMOJOModel(new H2OXGBoost())
  }

  test("Offset column gets propagated to MOJO model - GLM") {
    testOffsetColumnGetsPropagatedToMOJOModel(new H2OGLM())
  }

  test("Offset column gets propagated to MOJO model - DeepLearning") {
    testOffsetColumnGetsPropagatedToMOJOModel(new H2ODeepLearning())
  }

  private def testDeserializedMOJOAndOriginalMOJOReturnSameResult(
      algo: H2OSupervisedAlgorithm[_ <: Model.Parameters]): Unit = {
    val offsetColumn = "PSA"
    algo
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
      .setOffsetCol(offsetColumn)

    val pipeline = new Pipeline().setStages(Array(algo))

    val model = pipeline.fit(trainingDataset)
    val path = s"ml/build/deserialized_mojo_produce_same_result_${algo.getClass.getSimpleName}"
    model.write.overwrite().save(path)
    val loadedModel = PipelineModel.load(path)

    val originalResult = model.transform(testingDataset)
    val deserializedResult = loadedModel.transform(testingDataset)

    TestUtils.assertDataFramesAreIdentical(originalResult, deserializedResult)
  }

  test("The original MOJO and deserialized MOJO return the same result - GBM") {
    testDeserializedMOJOAndOriginalMOJOReturnSameResult(new H2OGBM())
  }

  test("The original MOJO and deserialized MOJO return the same result - XGBoost") {
    testDeserializedMOJOAndOriginalMOJOReturnSameResult(new H2OXGBoost())
  }

  test("The original MOJO and deserialized MOJO return the same result - GLM") {
    testDeserializedMOJOAndOriginalMOJOReturnSameResult(new H2OGLM())
  }

  test("The original MOJO and deserialized MOJO return the same result - DeepLearning") {
    testDeserializedMOJOAndOriginalMOJOReturnSameResult(new H2ODeepLearning())
  }

  private def testMOJOWithSetOffsetColumnTakesItIntoAccount(
      algo: H2OSupervisedAlgorithm[_ <: Model.Parameters]): Unit = {
    val offsetColumn = "PSA"
    algo
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setOffsetCol(offsetColumn)

    val model = algo.fit(trainingDataset)

    def extractResultAndOffset(model: H2OSupervisedMOJOModel, testingDataset: Dataset[Row]): Array[(Double, Double)] = {
      val idColumn = "ID"
      model
        .transform(testingDataset)
        .sort(idColumn)
        .select("prediction", offsetColumn)
        .collect()
        .map(row => (row.getDouble(0), row.getDouble(1)))
    }

    val testingDatasetWithOffsetZeroed = testingDataset.withColumn(offsetColumn, lit(0d))

    val resultsWithOffset = extractResultAndOffset(model, testingDataset)
    val resultsWithoutOffset = extractResultAndOffset(model, testingDatasetWithOffsetZeroed)

    resultsWithOffset should not equal resultsWithoutOffset
    resultsWithOffset.zip(resultsWithoutOffset).foreach {
      case ((resultWithOffset, offset), (resultWithoutOffset, _)) =>
        resultWithOffset - offset shouldEqual resultWithoutOffset +- 0.0001
    }
  }

  test("The MOJO model with set offsetColumn takes it into account - GBM") {
    testMOJOWithSetOffsetColumnTakesItIntoAccount(new H2OGBM())
  }

  test("The MOJO model with set offsetColumn takes it into account - XGBoost") {
    testMOJOWithSetOffsetColumnTakesItIntoAccount(new H2OXGBoost())
  }

  test("The MOJO model with set offsetColumn takes it into account - GLM") {
    testMOJOWithSetOffsetColumnTakesItIntoAccount(new H2OGLM())
  }

  // TODO: Enable test once PUBDEV-7067 is resolved.
  // Setting offset on DeepLearningMojoModel doesn't take effect.
  ignore("The MOJO model with set offsetColumn takes it into account - DeepLearning") {
    testMOJOWithSetOffsetColumnTakesItIntoAccount(new H2ODeepLearning())
  }

  test("Load K-means as supervised model fails") {
    val algo = new H2OKMeans()
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
    val model = algo.fit(dataset)
    val path = s"ml/build/load_kmeans_as_supervised_model_fails"
    model.write.overwrite().save(path)

    intercept[RuntimeException] {
      H2OSupervisedMOJOModel.load(path)
    }
  }

  private def testLoadingOfSuppervisedAlgorithmWorks(algo: H2OSupervisedAlgorithm[_ <: Model.Parameters]): Unit = {
    val offsetCol = "PSA"
    algo
      .setSeed(1)
      .setFeaturesCols("RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setOffsetCol(offsetCol)

    val model = algo.fit(dataset)
    val path = s"ml/build/load_supervised_model_${algo.getClass.getSimpleName}"
    model.write.overwrite().save(path)

    val loadedModel = H2OSupervisedMOJOModel.load(path)

    assert(loadedModel.getOffsetCol() == offsetCol)
  }

  test("Load GBM as supervised model works") {
    testLoadingOfSuppervisedAlgorithmWorks(new H2OGBM())
  }

  test("Load GLM as supervised model works") {
    testLoadingOfSuppervisedAlgorithmWorks(new H2OGLM())
  }
}
