package water.sparkling.itest.local

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningParameters
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
import water.sparkling.itest.{LocalTest, SparkITest}

/**
 * PUBDEV-928 test suite.
 *
 * Verifies that DL can be run on 0-length chunks.
 */
@RunWith(classOf[JUnitRunner])
class PubDev928TestSuite extends FunSuite with SparkITest {

  test("Verify scoring on 0-length chunks", LocalTest) {
    launch("water.sparkling.itest.local.PubDev928Test",
      env {
        sparkMaster("local-cluster[3,2,1024]")
      }
    )
  }
}

object PubDev928Test extends SparkContextSupport {

  def main(args: Array[String]): Unit = {
    val conf = configure("PUBDEV-928")
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val airlinesData = new H2OFrame(new java.io.File("examples/smalldata/allyears2k_headers.csv.gz"))

    val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
    airlinesTable.toDF.registerTempTable("airlinesTable")

    val query = "SELECT * FROM airlinesTable WHERE Dest LIKE 'SFO'"
    val result: H2OFrame = sqlContext.sql(query) // Using a registered context and table

    val train: H2OFrame = result('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
      'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
      'Distance, 'IsDepDelayed )
    //train.replace(train.numCols()-1, train.lastVec().toEnum)
    println(train.lastVec().naCnt())
    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._response_column = 'IsDepDelayed

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    // THIS WILL FAIL
    val testFrame : H2OFrame = result
    // Verify that testFrame has at least on chunk with 0-rows
    val av = testFrame.anyVec();
    assert( (0 until av.nChunks()).exists(idx => av.chunkForChunkIdx(idx).len() == 0), "At least on chunk with 0-rows has to exist!")

    val predictionH2OFrame = dlModel.score(testFrame)('predict)
    assert(predictionH2OFrame.numRows() == testFrame.numRows())
  }
}