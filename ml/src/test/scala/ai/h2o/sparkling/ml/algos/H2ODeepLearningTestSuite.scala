package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.H2ODeepLearningMOJOModel
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2ODeepLearningTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/insurance.csv"))

  test("Test H2ODeepLearning") {

    import spark.implicits._

    val algo = new H2ODeepLearning()
      .setDistribution("tweedie")
      .setHidden(Array(1))
      .setEpochs(1000)
      .setTrainSamplesPerIteration(-1)
      .setReproducible(true)
      .setActivation("Tanh")
      .setSingleNodeMode(false)
      .setBalanceClasses(false)
      .setForceLoadBalance(false)
      .setSeed(23123)
      .setTweediePower(1.5)
      .setScoreTrainingSamples(0)
      .setColumnsToCategorical("District")
      .setScoreValidationSamples(0)
      .setStoppingRounds(0)
      .setFeaturesCols("District", "Group", "Age")
      .setLabelCol("Claims")

    val model: H2ODeepLearningMOJOModel = algo.fit(dataset)

    val layers = model.getModelSummary()

    layers.select("Type").as[String].collect() shouldBe Seq("Input", "Tanh", "Linear")

    val result = model.transform(dataset)

    result.count() shouldBe dataset.count()
  }
}
