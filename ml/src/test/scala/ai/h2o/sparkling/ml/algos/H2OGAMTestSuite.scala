package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.Key

@RunWith(classOf[JUnitRunner])
class H2OGAMTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  test("Test H2OGAM Pipeline") {

    val algo = new H2OGAM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("DPROS", "DCAPS", "RACE", "GLEASON")
      .setGamCols(Array(Array("PSA"), Array("AGE")))
      .setLabelCol("CAPSULE")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/gam_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/gam_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/gam_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/gam_pipeline_model")

    loadedModel.transform(dataset).count()
  }
}
