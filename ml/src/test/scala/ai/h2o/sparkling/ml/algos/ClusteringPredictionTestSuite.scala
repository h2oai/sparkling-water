package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.metrics.{H2OClusteringMetrics, H2OMetrics, MetricsAssertions}
import ai.h2o.sparkling.ml.models.{H2OKMeansMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ClusteringPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))

  test("predictionCol content") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset)

    // the 'cluster' from clustering prediction is directly in predictionCol
    assert(transformed.select("prediction").head() == Row(0))
    assert(transformed.select("prediction").distinct().count() == 3)
  }

  test("detailedPredictionCol content") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset)

    val expectedCols = Seq("cluster", "distances")
    assert(transformed.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))

    assert(transformed.select("detailed_prediction.cluster").head().getInt(0) == 0)
    assert(transformed.select("detailed_prediction.distances").head().getAs[Seq[Double]](0).length == 3)
  }

  test("transformSchema") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", IntegerType, nullable = true)

    val clusterField = StructField("cluster", IntegerType, nullable = true)
    val distancesField = StructField("distances", ArrayType(DoubleType, containsNull = false), nullable = true)
    val detailedPredictionColField =
      StructField("detailed_prediction", StructType(clusterField :: distancesField :: Nil), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

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
    metricsObject.isInstanceOf[H2OClusteringMetrics] should be(true)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(metricsObject, metrics)
  }

  test("test clustering metric objects") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
    val model = algo.fit(dataset)
    assertMetrics[H2OClusteringMetrics](model)

    model.write.overwrite().save("ml/build/clustering_model_metrics")
    val loadedModel = H2OKMeansMOJOModel.load("ml/build/clustering_model_metrics")
    assertMetrics[H2OClusteringMetrics](loadedModel)
  }
}
