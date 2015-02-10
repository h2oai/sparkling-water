package water.sparkling.itest.yarn

import java.io.File

import org.apache.spark.h2o.H2OContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.DataFrame
import water.sparkling.itest.{SparkITest, YarnTest}

/**
 * Created by michal on 2/9/15.
 */
@RunWith(classOf[JUnitRunner])
class KMeansITestSuite extends FunSuite with SparkITest {

  test("MLlib KMeans on airlines_all data", YarnTest) {
    launch( "water.sparkling.itest.yarn.KMeansITest",
      env {
        sparkMaster("yarn-client")
        // Configure YARN environment
        conf("spark.yarn.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 3) // 3 executor instances
        conf("spark.executor.memory", "5g") // 5g per executor
      }
    )
  }
}

/**
 * Test runner loading large airlines data from YARN HDFS via H2O API
 * transforming them into RDD and launching MLlib K-means.
 */
object KMeansITest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansITest")
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()

    import h2oContext._
    // Import all year airlines into H2O
    val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
    val timer1 = new water.util.Timer
    //val d = new java.net.URI(new File(path))
    val airlinesData = new DataFrame(new File(path))
    val timeToParse = timer1.time

    // Transfer data from H2O to Spark RDD

    import org.apache.spark.sql.SQLContext

    val timer2 = new water.util.Timer
    implicit val sqlContext = new SQLContext(sc)
    val airlinesRDD = asSchemaRDD(airlinesData)(sqlContext)
    val timeToTransfer = timer2.time

    assert (airlinesData.numRows == airlinesRDD.count, "Transfer of H2ORDD to SparkRDD completed!")

    // Run Kmeans in Spark  on indices 10,19,26 (FlightNo, Distance, WeatherDelay)

    val airlinesVectorRDD = airlinesRDD.map(row => Vectors.dense(row.getByte(1) * 1.0, row.getByte(2) * 1.0, row.getByte(3) * 1.0))

    val timer3 = new water.util.Timer
    val clusters = KMeans.train(airlinesVectorRDD, 5, 20)
    val timeForKMModel = timer3.time

    // Evaluate clustering by computing within set sum of squared errors
    val WSSSE = clusters.computeCost(airlinesVectorRDD)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    // Shutdown Spark
    sc.stop()
  }
}
