package ai.h2o.sparkling.ml.metrics

import ai.h2o.sparkling.ml.algos.H2OKMeans
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ClusteringMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))

  private lazy val Array(trainingDataset, validationDataset) = dataset.randomSplit(Array(0.8, 0.2), seed = 42)

  test("test calculation of kmeans metric objects on arbitrary dataset") {
    val algorithm = new H2OKMeans()
      .setValidationDataFrame(validationDataset)
      .setSeed(1)
      .setK(3)
      .setUserPoints(Array(Array(4.9, 3.0, 1.4, 0.2), Array(5.6, 2.5, 3.9, 1.1), Array(6.5, 3.0, 5.2, 2.0)))
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algorithm.fit(trainingDataset)

    val trainingMetrics = model.getMetrics(trainingDataset)
    val trainingMetricsObject = model.getMetricsObject(trainingDataset)
    val validationMetrics = model.getMetrics(validationDataset)
    val validationMetricsObject = model.getMetricsObject(validationDataset)
    val expectedTrainingMetrics = model.getTrainingMetrics()
    val expectedValidationMetrics = model.getValidationMetrics()

    MetricsAssertions.assertEqual(expectedTrainingMetrics, trainingMetrics, tolerance = 0.00001)
    MetricsAssertions.assertEqual(expectedValidationMetrics, validationMetrics)
    val ignoredGetters = Set("getCustomMetricValue", "getScoringTime")
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(trainingMetricsObject, trainingMetrics, ignoredGetters)
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(validationMetricsObject, validationMetrics, ignoredGetters)
  }
}
