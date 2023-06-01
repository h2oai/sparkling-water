package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.metrics.{H2ORegressionCoxPHMetrics, MetricsAssertions}
import ai.h2o.sparkling.ml.models.H2OCoxPHMOJOModel
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.Key

@RunWith(classOf[JUnitRunner])
class H2OCoxPHTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/coxph_test/heart.csv"))

  private lazy val datasetTest = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/coxph_test/heart_test.csv"))

  test("Test H2OCoxPH Pipeline") {
    val algo = new H2OCoxPH()
      .setStartCol("start")
      .setStopCol("stop")
      .setLabelCol("event")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/cox_ph_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/cox_ph_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/cox_ph_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/cox_ph_pipeline_model")

    loadedModel.transform(dataset).count()
  }

  test("H2OCoxPH with set modelId is trained multiple times") {
    val modelId = "testingH2OCoxPHModel"

    val key1 = Key.make(modelId).toString
    val key2 = Key.make(modelId + "_1").toString
    val key3 = Key.make(modelId + "_2").toString

    // Create  model
    val algo = new H2OCoxPH()
      .setModelId(modelId)
      .setStartCol("start")
      .setStopCol("stop")
      .setLabelCol("event")
      .setKeepBinaryModels(true)

    H2OModel.modelExists(key1) shouldBe false

    algo.fit(dataset)
    print(H2OModel.listAllModels().mkString("Array(", ", ", ")"))
    H2OModel.modelExists(key1) shouldBe true
    H2OModel.modelExists(key2) shouldBe false
    H2OModel.modelExists(key3) shouldBe false

    algo.fit(dataset)
    H2OModel.modelExists(key1) shouldBe true
    H2OModel.modelExists(key2) shouldBe true
    H2OModel.modelExists(key3) shouldBe false

    algo.fit(dataset)
    H2OModel.modelExists(key1) shouldBe true
    H2OModel.modelExists(key2) shouldBe true
    H2OModel.modelExists(key3) shouldBe true
  }

  test("predictions have right values") {
    val algo = new H2OCoxPH()
      .setStartCol("start")
      .setStopCol("stop")
      .setLabelCol("event")
      .setIgnoredCols(Array("id"))

    val model = algo.fit(dataset)
    val predictions = model.transform(dataset).select("prediction")

    predictions.count() shouldBe dataset.count()
    predictions.filter("prediction is not null").count() shouldBe dataset.count()
    predictions.first().getDouble(0) shouldBe (0.20032351116082292 +- 0.0001)
  }

  private def assertMetrics(model: H2OCoxPHMOJOModel): Unit = {
    assertMetrics(model.getTrainingMetricsObject(), model.getTrainingMetrics())
    assert(model.getValidationMetricsObject() == null)
    assert(model.getValidationMetrics() == Map())
    assert(model.getCrossValidationMetricsObject() == null)
    assert(model.getCrossValidationMetrics() == Map())
  }

  private def assertMetrics(metricsObject: H2ORegressionCoxPHMetrics, metrics: Map[String, Double]): Unit = {
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(metricsObject, metrics)
  }

  test("test metric objects") {
    val algo = new H2OCoxPH()
      .setStartCol("start")
      .setStopCol("stop")
      .setLabelCol("event")
      .setIgnoredCols(Array("id"))
    val model = algo.fit(dataset)
    assertMetrics(model)

    model.write.overwrite().save("ml/build/coxph_model_metrics")
    val loadedModel = H2OCoxPHMOJOModel.load("ml/build/coxph_model_metrics")
    assertMetrics(loadedModel)
  }
}
