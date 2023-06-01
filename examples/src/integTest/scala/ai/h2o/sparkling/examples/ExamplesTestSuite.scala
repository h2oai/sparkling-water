package ai.h2o.sparkling.examples

import ai.h2o.sparkling.SharedH2OTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExamplesTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local-cluster[2,1,2024]")

  test("Prostate Demo") {
    ProstateDemo.main(Array.empty)
  }

  test("Ham or Spam Demo") {
    HamOrSpamDemo.main(Array.empty)
  }

  test("Deep Learning Demo") {
    DeepLearningDemo.main(Array.empty)
  }

  test("Craigslist Job Titles Demo") {
    CraigslistJobTitlesApp.main(Array.empty)
  }

  test("City Bike Sharing Demo") {
    CityBikeSharingDemo.main(Array.empty)
  }

  test("Chicago Crime Demo") {
    ChicagoCrimeApp.main(Array.empty)
  }

  test("Airlines with Weather Demo") {
    AirlinesWithWeatherDemo.main(Array.empty)
  }
}
