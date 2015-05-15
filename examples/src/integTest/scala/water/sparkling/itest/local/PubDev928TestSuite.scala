package water.sparkling.itest.local

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.examples.h2o.Airlines
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.h2o.{DataFrame, H2OContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Tokenizer, HashingTF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.Vec
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

object PubDev928Test {

  def main(args: Array[String]): Unit = {
    val conf = configure("PUBDEV-928")
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val airlinesData = new DataFrame(new java.io.File("examples/smalldata/allyears2k_headers.csv.gz"))

    val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
    airlinesTable.registerTempTable("airlinesTable")

    val query = "SELECT * FROM airlinesTable WHERE Dest LIKE 'SFO'"
    val result = sql(query) // Using a registered context and table

    val train: DataFrame = result( 'Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
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
    val testFrame : DataFrame = result
    // Verify that testFrame has at least on chunk with 0-rows
    val av = testFrame.anyVec();
    assert( (0 until av.nChunks()).exists(idx => av.chunkForChunkIdx(idx).len() == 0), "At least on chunk with 0-rows has to exist!")

    val predictionH2OFrame = dlModel.score(testFrame)('predict)
    assert(predictionH2OFrame.numRows() == testFrame.numRows())
  }
}