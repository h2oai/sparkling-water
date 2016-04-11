package water.sparkling.itest.local

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.SparkContext
import org.apache.spark.examples.h2o.Airlines
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.app.SparkContextSupport
import water.fvec.H2OFrame
import water.sparkling.itest.{LocalTest, IntegTestHelper}

/**
  * PUBDEV-928 test suite.
  *
  * Verifies that DL can be run on 0-length chunks.
  */
@RunWith(classOf[JUnitRunner])
class PubDev928Suite extends FunSuite with IntegTestHelper {

  test("Verify scoring on 0-length chunks", LocalTest) {
    launch("water.sparkling.itest.local.PubDev928Test",
      env {
        sparkMaster("local-cluster[3,2,2048]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object PubDev928Test extends SparkContextSupport {

  def main(args: Array[String]): Unit = {
    val conf = configure("PUBDEV-928")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val airlinesData = new H2OFrame(new java.io.File("examples/smalldata/allyears2k_headers.csv.gz"))

    // We need to explicitly repartition data to 12 chunks/partitions since H2O parser handling
    // partitioning dynamically based on available number of CPUs
    println("Number of chunks before query: " + airlinesData.anyVec().nChunks())
    val airlinesTable: RDD[Airlines] = asRDD[Airlines](airlinesData)
    airlinesTable.toDF.registerTempTable("airlinesTable")

    val query = "SELECT * FROM airlinesTable WHERE Dest LIKE 'SFO'"
    // Transform result of SQL query directly into H2OFrame, but change number of
    val queryResult = sqlContext.sql(query)
    val partitionNumbers = queryResult.count().asInstanceOf[Int] + 1
    val result: H2OFrame = h2oContext.asH2OFrame(queryResult.repartition(partitionNumbers), "flightTable")
    println("Number of partitions in query result: " + queryResult.rdd.partitions.size)
    println("Number of chunks in query result" + result.anyVec().nChunks())

    val train: H2OFrame = result('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
      'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
      'Distance, 'IsDepDelayed)
    train.replace(train.numCols() - 1, train.lastVec().toCategoricalVec)
    train.update()
    println(s"Any vec chunk cnt: ${train.anyVec().nChunks()}")
    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._response_column = 'IsDepDelayed

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    // THIS WILL FAIL
    val testFrame: H2OFrame = result
    // Verify that testFrame has at least one chunk with 0-rows
    val av = testFrame.anyVec()
    println(s"Test frame chunk cnt: ${av.nChunks()}")
    for (i <- 0 until av.nChunks()) {
      println(av.chunkForChunkIdx(i).len())
    }
    assert((0 until av.nChunks()).exists(idx => av.chunkForChunkIdx(idx).len() == 0), "At least on chunk with 0-rows has to exist!")

    // And run scoring on dataset which contains at least one chunk with zero-lines
    val predictionH2OFrame = dlModel.score(testFrame)('predict)
    assert(predictionH2OFrame.numRows() == testFrame.numRows())

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
