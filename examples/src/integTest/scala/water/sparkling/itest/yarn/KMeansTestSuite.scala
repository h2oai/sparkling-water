package water.sparkling.itest.yarn

import hex.kmeans.KMeansModel.KMeansParameters
import org.apache.spark.h2o._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestHelper, YarnTest}
import water.util.Timer

@RunWith(classOf[JUnitRunner])
class KMeansITestSuite extends FunSuite with IntegTestHelper {

  test("MLlib KMeans on airlines_all data", YarnTest) {
    launch("water.sparkling.itest.yarn.KMeansITest",
      env {
        sparkMaster("yarn-client")
        // Configure YARN environment
        conf("spark.yarn.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 6) // 10 executor instances
        conf("spark.executor.memory", "8g") // 20g per executor
        conf("spark.ext.h2o.port.base", 63331) //Start at baseport 63331
        conf("spark.driver.memory", "8g")
        conf("spark.executor.cores", 32) //Use up all the cores on the machines
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
    val conf = new SparkConf().setAppName("KMeansITest")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._

    // Import all year airlines into H2O
    val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
    val timer1 = new water.util.Timer
    val d = new java.net.URI(path)
    val airlinesData = new H2OFrame(d)
    val timeToParse = timer1.time

    // Run Kmeans in H2O
    val ignore_columns = airlinesData.names diff Array("Month", "DayofMonth", "DayOfWeek")
    val H2OKMTimer = new Timer
    val kmeansParams = new KMeansParameters()
    kmeansParams._k = 5
    kmeansParams._max_iterations = 10
    kmeansParams._train = airlinesData
    kmeansParams._ignored_columns = ignore_columns
    kmeansParams._standardize = false
    val KmeansModel = new hex.kmeans.KMeans(kmeansParams).trainModel().get()
    val H2OKMBuildTime = H2OKMTimer.time

    // Score in H2O
    import org.apache.spark.sql.SQLContext
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val pred = KmeansModel.score(airlinesData)
    val predDF = asDataFrame(pred)
    val clusterCounts = predDF.rdd.countByValue()

    // Run Kmeans in Spark
    val sqlQueryTimer = new water.util.Timer
    val airlinesDF = asDataFrame(airlinesData)(sqlContext)
    airlinesDF.registerTempTable("airlinesRDD")
    val airlinesTable = sqlContext.sql(
      """SELECT Month, DayofMonth, DayOfWeek FROM airlinesRDD"""
    )
    val sqlQueryTime = sqlQueryTimer.time

    assert(airlinesData.numRows == airlinesTable.count, "Transfer of H2ORDD to SparkRDD completed!")

    // Run Kmeans in Spark  on indices 10,19,26 (FlightNo, Distance, WeatherDelay)

    val airlinesVectorRDD = airlinesTable.map(row => Vectors.dense(row.getByte(0) * 1.0, row.getByte(1) * 1.0, row.getByte(2) * 1.0))
    val SparkKMTimer = new water.util.Timer
    val clusters = KMeans.train(airlinesVectorRDD, 5, 10)
    val SparkKMBuildTime = SparkKMTimer.time

    // Predict on Spark's Kmeans
    val sparkPredRDD = clusters.predict(airlinesVectorRDD)
    val srdd: DataFrame = sparkPredRDD.map(v => IntHolder(Option(v))).toDF
    val df: H2OFrame = srdd
    val sparkClusterCounts = sparkPredRDD.countByValue()

    // Get Within Set Sum of Squared Errors
    val sparkWSSSE = clusters.computeCost(airlinesVectorRDD)
    val h2oWSSSE = KmeansModel._output._withinss.fold(0.0)(_ + _)

    println("Spark: Within Set Sum of Squared Errors = " + sparkWSSSE)
    println("Spark: Time to Build (s) = " + SparkKMBuildTime)
    println("H2O: Within Set Sum of Squared Errors = " + h2oWSSSE)
    println("H2O: Time to Build (s) = " + H2OKMBuildTime)

    val relativeMeanDiff = (sparkWSSSE - h2oWSSSE) / sparkWSSSE
    assert(relativeMeanDiff < 0.1, "Within Set Sum of Squared Errors matches!")

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
