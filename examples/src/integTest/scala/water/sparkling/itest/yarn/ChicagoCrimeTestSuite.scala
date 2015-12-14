package water.sparkling.itest.yarn

import org.apache.spark.SparkContext
import org.apache.spark.examples.h2o.{ChicagoCrimeApp, Crime}
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.app.SparkContextSupport
import water.sparkling.itest.SparkITest

/**
 * Test following Alex's chicago crime demo.
 */
@RunWith(classOf[JUnitRunner])
class ChicagoCrimeTestSuite extends FunSuite with SparkITest {

  test("Chicago Crime Demo") {
    launch( "water.sparkling.itest.yarn.ChicagoCrimeTest",
      env {
        sparkMaster("yarn-client")
        // Configure YARN environment
        conf("spark.yarn.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 3)
        conf("spark.executor.memory", "8g")
        conf("spark.ext.h2o.port.base", 63331)
        conf("spark.driver.memory", "8g")
      }
    )
  }
}

object ChicagoCrimeTest extends SparkContextSupport {

  def main(args: Array[String]): Unit = {
    try {
      test(args)
    } catch {
      case t:Throwable => {
        System.err.println(t.toString)
        System.err.println(t.getStackTrace.toString)
        water.H2O.exit(-1)
      }
    }
  }

  def test(args: Array[String]) {
    val sc = new SparkContext(configure("ChicagoCrimeTest"))
    // SQL support
    val sqlContext = new SQLContext(sc)
    // Start H2O services
    val h2oContext = H2OContext.getOrCreate(sc)

    val app = new ChicagoCrimeApp(
      weatherFile = "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoAllWeather.csv",
      censusFile =  "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoCensus.csv",
      crimesFile =  "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoCrimes.csv")(sc, sqlContext, h2oContext)
    // Load data
    val (weatherTable, censusTable, crimesTable) = app.loadAll()
    // Train model
    val (gbmModel, dlModel) = app.train(weatherTable, censusTable, crimesTable)

    val crimeExamples = Seq(
      Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET",false, 422, 4, 7, 46, 18),
      Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE",false, 923, 9, 14, 63, 11))

    for (crime <- crimeExamples) {
      val arrestProbGBM = 100*app.scoreEvent(crime,
        gbmModel,
        censusTable)(sqlContext, h2oContext)
      val arrestProbDL = 100*app.scoreEvent(crime,
        dlModel,
        censusTable)(sqlContext, h2oContext)
      println(
        s"""
           |Crime: $crime
            |  Probability of arrest best on DeepLearning: ${arrestProbDL} %
            |  Probability of arrest best on GBM: ${arrestProbGBM} %                                                                |
        """.stripMargin)
    }

    // Shutdown full stack
    app.shutdown()
  }

}
