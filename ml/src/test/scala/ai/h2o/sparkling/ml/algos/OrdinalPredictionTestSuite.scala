package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.metrics.{H2OMetrics, H2OOrdinalGLMMetrics, H2OOrdinalMetrics, MetricsAssertions}
import ai.h2o.sparkling.ml.models.{H2OGLMMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class OrdinalPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/insurance.csv"))

  private def createAlgorithm(): H2OGLM = {
    new H2OGLM()
      .setSplitRatio(0.8)
      .setFeaturesCols("District", "Group", "Claims")
      .setLabelCol("Age")
      .setSeed(1)
      .setFamily("ordinal")
  }

  private def assertExistenceOfColumns(df: DataFrame, path: String, expectedColumns: Seq[String]): Unit = {
    assert(df.select(path).columns.sameElements(expectedColumns))
  }

  test("Correct content of ordinal predictions") {
    val algorithm = createAlgorithm()
    val model = algorithm.fit(dataset)

    val predictions = model.transform(dataset)

    assert(model.getModelDetails().contains(""""model_category": "Ordinal""""))
    assertExistenceOfColumns(predictions, "*", dataset.columns ++ Seq("detailed_prediction", "prediction"))
    assertExistenceOfColumns(predictions, "detailed_prediction.*", Seq("label", "probabilities"))
    val probabilities = predictions.select("detailed_prediction.probabilities").take(2).map(_.getStruct(0))

    assert(probabilities(0).getDouble(0) != probabilities(1).getDouble(0))
    assert(probabilities(0).getDouble(1) != probabilities(1).getDouble(1))
    assert(probabilities(0).getDouble(2) != probabilities(1).getDouble(2))
    assert(probabilities(0).getDouble(3) != probabilities(1).getDouble(3))
  }

  test("transformSchema returns expected result") {
    val algorithm = createAlgorithm()
    val model = algorithm.fit(dataset)

    val datasetFields = dataset.schema.fields
    val labelField = StructField("label", StringType, nullable = true)
    val predictionColField = StructField("prediction", StringType, nullable = true)
    val classFields = Seq("25-29", "30-35", "<25", ">35").map(StructField(_, DoubleType, nullable = false))
    val probabilitiesField = StructField("probabilities", StructType(classFields), nullable = false)
    val detailedPredictionColField =
      StructField("detailed_prediction", StructType(Seq(labelField, probabilitiesField)), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(model.getModelDetails().contains(""""model_category": "Ordinal""""))
    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }

  private def assertMetrics[T](model: H2OMOJOModel): Unit = {
    assertMetrics(model.getTrainingMetricsObject(), model.getTrainingMetrics())
    assertMetrics(model.getValidationMetricsObject(), model.getValidationMetrics())
    assert(model.getCrossValidationMetricsObject() == null)
    assert(model.getCrossValidationMetrics() == Map())
  }

  private def assertMetrics(metricsObject: H2OMetrics, metrics: Map[String, Double]): Unit = {
    metricsObject.isInstanceOf[H2OOrdinalGLMMetrics] should be(true)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(metricsObject, metrics)
  }

  test("test ordinal glm metric objects") {
    val algo = createAlgorithm()
    val model = algo.fit(dataset)
    assertMetrics[H2OOrdinalMetrics](model)

    model.write.overwrite().save("ml/build/glm_ordinal_model_metrics")
    val loadedModel = H2OGLMMOJOModel.load("ml/build/glm_ordinal_model_metrics")
    assertMetrics[H2OOrdinalGLMMetrics](loadedModel)
  }
}
