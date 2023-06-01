package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OStackedEnsembleTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("CAPSULE", col("CAPSULE").cast("string"))

  private lazy val trainingDataset = dataset.where("ID <= 300").cache()
  private lazy val testingDataset = dataset.where("ID > 300").cache()

  private val foldsNo = 5

  test("Create Stacked Ensemble model using cross validations") {

    val glm = getGlm()
    val gbm = getGbm()

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(glm, gbm))
      .setMetalearnerAlgorithm("GLM")
      .setLabelCol("CAPSULE")
      .setSeed(42)
      .setScoreTrainingSamples(0)

    val ensembleModel = ensemble.fit(trainingDataset)

    ensembleModel.getMetalearnerAlgorithm() shouldBe "glm"

    val predictions = ensembleModel
      .transform(testingDataset)
      .select("detailed_prediction")

    predictions.distinct().count() shouldBe 80
  }

  test("Create Stacked Ensemble using blending frame") {

    val trainingDF = trainingDataset.where("ID <= 200")
    val blendingDF = trainingDataset.where("ID > 200")

    val drf = new H2ODRF()
      .setLabelCol("CAPSULE")
      .setKeepBinaryModels(true)
      .setSeed(42)

    val gbm = new H2OGBM()
      .setLabelCol("CAPSULE")
      .setKeepBinaryModels(true)
      .setSeed(42)

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(drf, gbm))
      .setBlendingDataFrame(blendingDF)
      .setLabelCol("CAPSULE")
      .setSeed(42)

    val ensembleModel = ensemble.fit(trainingDF)

    val predictions = ensembleModel
      .transform(testingDataset)
      .select("detailed_prediction")

    predictions.distinct().count() shouldBe 57
  }

  test("H2O StackedEnsemble deletes base models") {

    val drf = getDrf()
    val gbm = getGbm()

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(drf, gbm))
      .setLabelCol("CAPSULE")
      .setSeed(42)

    H2OModel.listAllModels() should have length 0

    ensemble.fit(dataset)

    H2OModel.listAllModels() should have length 0
  }

  test("H2O Stacked Ensemble pipeline serialization and deserialization") {

    val drf = getDrf()
    val gbm = getGbm()

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(drf, gbm))
      .setLabelCol("CAPSULE")
      .setSeed(42)

    val pipeline = new Pipeline().setStages(Array(ensemble))
    pipeline.write.overwrite().save("ml/build/stacked_ensemble_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/stacked_ensemble_pipeline")

    val model = loadedPipeline.fit(trainingDataset)

    model.write.overwrite().save("ml/build/stacked_ensemble_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/stacked_ensemble_pipeline_model")

    val predictions = loadedModel
      .transform(testingDataset)
      .select("detailed_prediction")

    predictions.distinct().count() shouldBe 80
  }

  private def getGbm() = {
    new H2OGBM()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setSeed(42)
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }

  private def getGlm() = {
    new H2OGLM()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setSeed(42)
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }

  private def getDrf() = {
    new H2ODRF()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setSeed(42)
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }
}
