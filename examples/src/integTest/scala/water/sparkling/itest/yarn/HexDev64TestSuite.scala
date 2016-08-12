package water.sparkling.itest.yarn

import org.apache.spark.h2o._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestStopper, IntegTestHelper}

@RunWith(classOf[JUnitRunner])
class HexDev64TestSuite extends FunSuite with IntegTestHelper {

  test("HEX-DEV 64 test - airlines on big data") {
    launch("water.sparkling.itest.yarn.HexDev64Test",
      env {
        sparkMaster("yarn-client")
        // Configure YARN environment
        conf("spark.yarn.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 6) // 10 executor instances
        conf("spark.executor.memory", "8g") // 20g per executor
        conf("spark.ext.h2o.port.base", 63331)
        conf("spark.driver.memory", "8g")
      }
    )
  }
}

object HexDev64Test extends IntegTestStopper{

  def main(args: Array[String]): Unit = exitOnException{
    val conf = new SparkConf().setAppName("HexDev64Test")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)

    import h2oContext._
    // Import all year airlines into H2O
    val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
    val timer1 = new water.util.Timer
    val d = new java.net.URI(path)
    val airlinesData = new H2OFrame(d)
    val timeToParse = timer1.time / 1000
    println("Time it took to parse 116 million airlines = " + timeToParse + "secs")

    // Transfer data from H2O to Spark RDD

    import org.apache.spark.sql.SQLContext

    val timer2 = new water.util.Timer
    implicit val sqlContext = new SQLContext(sc)
    val airlinesDataFrame = asDataFrame(airlinesData)(sqlContext)
    val timeToTransfer = timer2.time / 1000
    println("Time it took to convert data to SparkRDD = " + timeToTransfer + "secs")

    assert(airlinesData.numRows == airlinesDataFrame.count, "Transfer of H2ORDD to SparkRDD completed!")

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
