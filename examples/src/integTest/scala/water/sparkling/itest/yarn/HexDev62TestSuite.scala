package water.sparkling.itest.yarn

import org.apache.spark.examples.h2o.AirlinesParse
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.IntegTestHelper

/**
  * Test suite for given JIRA bug.
  */
@RunWith(classOf[JUnitRunner])
class HexDev62TestSuite extends FunSuite with IntegTestHelper {

  ignore("HEX-DEV 62 test") {
    launch("water.sparkling.itest.yarn.HexDev62Test",
      env {
        sparkMaster("yarn-client")
        // Configure YARN environment
        conf("spark.yarn.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 5) // 10 executor instances
        conf("spark.executor.memory", "10g") // 20g per executor
        conf("spark.ext.h2o.port.base", 63331) //Start at baseport 63331
        conf("spark.driver.memory", "2g")
        conf("spark.executor.cores", 32) //Use up all the cores on the machines
      }
    )
  }
}

object HexDev62Test {

  def main(args: Array[String]): Unit = {
    try {
      test(args)
    } catch {
      case t: Throwable => {
        System.err.println(t.toString)
        t.printStackTrace()
        water.H2O.exit(-1)
      }
    }
  }

  def test(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HexDev62Test")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext.implicits._

    // Import all year airlines into SPARK
    implicit val sqlContext = new SQLContext(sc)
    val timer1 = new water.util.Timer
    val path = "hdfs://mr-0xd6-precise1.0xdata.loc:8020/datasets/airlines/airlines_all.csv"
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
