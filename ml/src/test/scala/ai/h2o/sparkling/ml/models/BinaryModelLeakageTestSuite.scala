package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import ai.h2o.sparkling.ml.algos.{H2OAutoML, H2OGBM, H2OGridSearch}
import ai.h2o.sparkling.ml.internals.H2OModel
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BinaryModelLeakageTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  private def referenceAlgo: H2OGBM =
    new H2OGBM()
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setKeepCrossValidationModels(true)
      .setNfolds(3)

  test("A regular algorithm with cross validation doesn't leave any model in DKV by default") {
    referenceAlgo.fit(dataset)
    H2OModel.listAllModels().length shouldEqual 0
  }

  test("Grid search doesn't leave any binary model in DKV by default") {
    val grid = new H2OGridSearch()
      .setAlgo(referenceAlgo)
      .setHyperParameters(Map("ntrees" -> Array(10, 20).map(_.asInstanceOf[AnyRef])))
    grid.fit(dataset)

    H2OModel.listAllModels().length shouldEqual 0
  }

  test("Acquiring model is available in H2O after training AutoML in SW") {
    val automl = new H2OAutoML()
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setKeepCrossValidationModels(true)
      .setNfolds(3)
      .setMaxModels(10)
    automl.fit(dataset)
    H2OModel.listAllModels().length shouldEqual 0
  }
}
