package water.sparkling.itest.yarn

import org.apache.spark.examples.h2o.{ChicagoCrimeApp, Crime}
import org.apache.spark.h2o._
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestHelper, IntegTestStopper}
import water.support.SparkContextSupport

/**
  * Test following Alex's chicago crime demo.
  */
@RunWith(classOf[JUnitRunner])
class ChicagoCrimeTestSuite extends FunSuite with IntegTestHelper {

  test("Chicago Crime Demo") {
    launch("water.sparkling.itest.yarn.ChicagoCrimeTest",
      env {
        sparkMaster("yarn-client")
        // Configure YARN environment
        conf("spark.yarn.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 3)
        conf("spark.executor.memory", "8g")
        conf("spark.ext.h2o.port.base", 63331)
        conf("spark.driver.memory", "8g")
        conf("spark.ext.h2o.hadoop.memory", "20G")
        isYarnIntegTest()
      }
    )
  }
}

object ChicagoCrimeTest extends SparkContextSupport with IntegTestStopper {

  def main(args: Array[String]): Unit = exitOnException{
    val spark = SparkSession.builder().appName("ChicagoCrimeTest").getOrCreate()
    // Start H2O services
    val hc = H2OContext.getOrCreate(spark)

    val app = new ChicagoCrimeApp(
      weatherFile = "hdfs://mr-0xd6.0xdata.loc/datasets/chicagoAllWeather.csv",
      censusFile = "hdfs://mr-0xd6.0xdata.loc/datasets/chicagoCensus.csv",
      crimesFile = "hdfs://mr-0xd6.0xdata.loc/datasets/chicagoCrimes.csv")(hc)

    // Load data
    val (weatherTable, censusTable, crimesTable) = app.loadAll()

    // Train model
    val (gbmModel, dlModel) = app.train(weatherTable, censusTable, crimesTable)

    // Score
    val crimes = Seq(
      Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET", domestic = false, 422, 4, 7, 46, 18),
      Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE", domestic = false, 923, 9, 14, 63, 11))
    app.score(crimes, gbmModel, dlModel, censusTable)
    spark.stop()
  }

}
