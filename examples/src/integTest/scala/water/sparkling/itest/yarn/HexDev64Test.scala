package water.sparkling.itest

import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.h2o._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.junit.JUnitRunner
import water.{DKV, Key}
import org.junit.runner.RunWith
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

@RunWith(classOf[JUnitRunner])
class HexDev64TestSuite extends FunSuite {

  val swassembly = sys.props.getOrElse("sparkling.test.assembly",
    fail("The variable 'sparkling.test.assembly' is not set! It should point to assembly jar file."))

  val testJar = sys.props.getOrElse("sparkling.test.jar", fail("The variable 'sparkling.test.jar' should "))

  test("Transfer of H2O Dataframe to SparkRDD") {
    // Requests environment:
    // - MASTER=YARN-CLIENT
    // - SPARK_HOME=spark-1.2.0-hadoop2.4
    //
    val cmdLine = Array[String]("--class", "water.sparkling.itest.HexDev64Test", "--jars", swassembly, testJar)
    println(cmdLine.mkString(","))
    SparkSubmit.main(cmdLine)
  }
}

object HexDev64Test {
  def main(args: Array[String]): Unit = {
    val swassembly = sys.props.getOrElse("sparkling.test.assembly",
      throw new IllegalArgumentException("The variable 'sparkling.test.assembly' is not set! It should point to assembly jar file."))
    val conf = new SparkConf().setAppName("HexDev64TestSuite").setJars(swassembly :: Nil)
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()

    import h2oContext._
    // Import all year airlines into H2O
    val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
    val timer1 = new water.util.Timer
    val d = new java.net.URI(path)
    val airlinesData = new DataFrame(d)
    val timeToParse = timer1.time/1000
    println("Time it took to parse 116 million airlines = " + timeToParse + "secs")

    // Transfer data from H2O to Spark RDD

    import org.apache.spark.sql.SQLContext

    val timer2 = new water.util.Timer
    implicit val sqlContext = new SQLContext(sc)
    val airlinesRDD = asSchemaRDD(airlinesData)(sqlContext)
    val timeToTransfer = timer2.time/1000
    println("Time it took to convert data to SparkRDD = " + timeToTransfer + "secs")

    assert (airlinesData.numRows == airlinesRDD.count, "Transfer of H2ORDD to SparkRDD completed!")
  }
}
