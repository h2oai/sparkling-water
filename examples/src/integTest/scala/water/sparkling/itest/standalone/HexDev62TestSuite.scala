package water.sparkling.itest.standalone

import org.apache.spark.examples.h2o.AirlinesParse
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestStopper, IntegTestHelper}


/**
  * Test for Jira Hex-Dev 62 : Import airlines data into Spark and then pass it to H2O.
  */
@RunWith(classOf[JUnitRunner])
class HexDev62TestSuite extends FunSuite with IntegTestHelper {

  ignore("HEX-DEV 62 test") {
    launch("water.sparkling.itest.standalone.HexDev62Test",
      env {
        // spark.master is passed via environment
        // Configure Standalone environment
        conf("spark.standalone.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 8)
        conf("spark.executor.memory", "7g")
        conf("spark.ext.h2o.cluster.size", 8)
      }
    )
  }
}

object HexDev62Test extends IntegTestStopper{

  def main(args: Array[String]): Unit = exitOnException{
    val conf = new SparkConf().setAppName("HexDev62Test")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext.implicits._

    // Import all year airlines into SPARK
    implicit val sqlContext = new SQLContext(sc)
    val path = "hdfs://mr-0xd6-precise1.0xdata.loc:8020/datasets/airlines/airlines_all.csv"
    val timer1 = new water.util.Timer
    val airlinesRaw = sc.textFile(path)
    val airlinesRDD = airlinesRaw.map(_.split(",")).map(row => AirlinesParse(row)).filter(!_.isWrongRow())
    val timeToParse = timer1.time / 1000
    println("Time it took to parse 116 million airlines = " + timeToParse + "secs")

    // Convert RDD to H2O Frame
    val timer2 = new water.util.Timer
    val airlinesData: H2OFrame = airlinesRDD
    val timeToH2O = timer2.time / 1000
    println("Time it took to transfer a Spark RDD to H2O Frame = " + timeToH2O + "secs")

    // Check H2OFrame is imported correctly
    assert(airlinesData.numRows == airlinesRDD.count, "Transfer of H2ORDD to SparkRDD completed!")

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}