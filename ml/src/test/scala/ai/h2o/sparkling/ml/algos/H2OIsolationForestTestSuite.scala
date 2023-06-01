package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OIsolationForestTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val trainingDataset = spark.read
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/anomaly/ecg_discord_train.csv"))

  private lazy val testingDataset = spark.read
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/anomaly/ecg_discord_test.csv"))

  test("H2OIsolationForest Pipeline serialization and deserialization") {
    val algo = new H2OIsolationForest()

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/isolation_forest_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/isolation_forest_pipeline")
    val model = loadedPipeline.fit(trainingDataset)
    val expected = model.transform(testingDataset)

    model.write.overwrite().save("ml/build/isolation_forest_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/isolation_forest_pipeline_model")
    val result = loadedModel.transform(testingDataset)

    TestUtils.assertDataFramesAreIdentical(expected, result)
  }
}
