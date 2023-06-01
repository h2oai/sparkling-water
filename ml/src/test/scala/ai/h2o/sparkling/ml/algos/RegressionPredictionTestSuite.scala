package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.algos
import ai.h2o.sparkling.ml.metrics.{H2OMetrics, H2ORegressionGLMMetrics, H2ORegressionMetrics, MetricsAssertions}
import ai.h2o.sparkling.ml.models.{H2OGBMMOJOModel, H2OGLMMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class RegressionPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  test("predictionCol content") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)

    val expectedCols = Seq("prediction")
    val predictions = model.transform(dataset)
    assert(predictions.select("prediction").schema.fields.map(_.name).sameElements(expectedCols))
  }

  test("detailedPredictionCol content") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset)

    val expectedCols = Seq("value", "contributions", "leafNodeAssignments", "stageResults")
    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))
    val contributions = predictions.select("detailed_prediction.contributions").head().getStruct(0)
    assert(contributions != null)
    assert(contributions.size == 8)
    val leafNodeAssignments = predictions.select("detailed_prediction.leafNodeAssignments").head().getSeq[String](0)
    assert(leafNodeAssignments != null)
    assert(leafNodeAssignments.length == algo.getNtrees())
    val stageResults = predictions.select("detailed_prediction.stageResults").head().getList(0)
    assert(stageResults != null)
    assert(stageResults.size() == algo.getNtrees())
  }

  test("contributions on unsupported algorithm will be ignored") {
    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)
    val result = model.transform(dataset)

    result.foreach(_ => {})
    result.select("detailed_prediction.*").columns should not contain "contributions"
  }

  test("leaf node assignments on unsupported algorithm will be ignored") {
    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithLeafNodeAssignments(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)
    val result = model.transform(dataset)

    result.foreach(_ => {})
    result.select("detailed_prediction.*").columns should not contain "leafNodeAssignments"
  }

  test(s"transformSchema") {
    val algo = new algos.H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val valueField = StructField("value", DoubleType, nullable = false)
    val predictionColField = StructField("prediction", DoubleType, nullable = true)
    val individualContributions =
      (algo.getFeaturesCols() :+ "BiasTerm").map(StructField(_, FloatType, nullable = false))
    val contributionsType = StructType(individualContributions)
    val contributionsField = StructField("contributions", contributionsType, nullable = false)
    val leafNodeAssignmentField =
      StructField("leafNodeAssignments", ArrayType(StringType, containsNull = false), nullable = false)
    val stageResultsField =
      StructField("stageResults", ArrayType(DoubleType, containsNull = false), nullable = false)
    val detailedPredictionColField = StructField(
      "detailed_prediction",
      StructType(valueField :: contributionsField :: leafNodeAssignmentField :: stageResultsField :: Nil),
      nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)
    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }

  private def assertMetrics[T](model: H2OMOJOModel): Unit = {
    assertMetrics[T](model.getTrainingMetricsObject(), model.getTrainingMetrics())
    assertMetrics[T](model.getValidationMetricsObject(), model.getValidationMetrics())
    assert(model.getCrossValidationMetricsObject() == null)
    assert(model.getCrossValidationMetrics() == Map())
  }

  private def assertMetrics[T](metricsObject: H2OMetrics, metrics: Map[String, Double]): Unit = {
    metricsObject.isInstanceOf[T] should be(true)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(metricsObject, metrics)
  }

  test("test regression metric objects") {
    val algo = new algos.H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = algo.fit(dataset)
    assertMetrics[H2ORegressionMetrics](model)

    model.write.overwrite().save("ml/build/gbm_regression_model_metrics")
    val loadedModel = H2OGBMMOJOModel.load("ml/build/gbm_regression_model_metrics")
    assertMetrics[H2ORegressionMetrics](loadedModel)
  }

  test("test regression glm metric objects") {
    val algo = new algos.H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    val model = algo.fit(dataset)
    assertMetrics[H2ORegressionGLMMetrics](model)

    model.write.overwrite().save("ml/build/glm_regression_model_metrics")
    val loadedModel = H2OGLMMOJOModel.load("ml/build/glm_regression_model_metrics")
    assertMetrics[H2ORegressionGLMMetrics](loadedModel)
  }
}
