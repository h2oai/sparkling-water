package water.sparkling.itest.standalone

import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.SparkITest

/**
 * Test for Parquet Import : Save small airlines data as Parquet File, 
 * import Parquet file into Spark as SchemaRDD, then run Deep Learning
 * using H2O*
 */
@RunWith(classOf[JUnitRunner])
class ParquetImportTestSuite extends FunSuite with SparkITest {

  test("Parquet File Import test") {
    launch( "water.sparkling.itest.standalone.ParquetImportTest",
      env {
        sparkMaster("spark://mr-0xd1-precise1.0xdata.loc:7077")
        // Configure Standalone environment
        conf("spark.standalone.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 8) // 8 executor instances
        conf("spark.executor.memory", "10g") // 10g per executor
        conf("spark.ext.h2o.cluster.size", 8) // 8 H2O nodes
      }
    )
  }
}

object ParquetImportTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetImportTest")

    // Launch H2O
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    implicit val sqlContext = new SQLContext(sc)

    // Import Parquet file into Spark as SchemaRDD
    val parquetFile = sqlContext.parquetFile("hdfs://mr-0xd6-precise1.0xdata.loc:8020/datasets/airlines/airlines.parquet")
    parquetFile.registerTempTable("parquetFile")

    // Check Parquet file copies correctly
    val AllFlights : DataFrame = parquetFile
    assert (AllFlights.numRows == parquetFile.count, "Transfer of H2ORDD to SparkRDD completed!")
    
    // Filter SchemaRdd and push to H2O as H2O DataFrame
    val ORDFlights = parquetFile.filter(r => r(17) == "ORD")
    assert (ORDFlights.count == 313943, "Correctly filtered out all ORD flights!")

    // Run Deep Learning on H2O Data Frame
    import hex.deeplearning.DeepLearning
    import hex.deeplearning.DeepLearningModel.DeepLearningParameters
    val dlParams = new DeepLearningParameters()
    dlParams._epochs = 10
    dlParams._train = ORDFlights
    dlParams._response_column = 'IsDepDelayed
    // Create a job
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    // Use model to estimate delay on training data
    val predictionH2OFrame = dlModel.score(ORDFlights)('predict)
    val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

    sc.stop()
    // Shutdown H2O explicitly (at least the driver)
    water.H2O.shutdown()
  }
}

