package water.sparkling.itest.standalone

import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.IntegTestHelper

/**
  * Test for Parquet Import : Save small airlines data as Parquet File,
  * import Parquet file into Spark as DataFrame, then run Deep Learning
  * using H2O*
  */
@RunWith(classOf[JUnitRunner])
class ParquetImportTestSuite extends FunSuite with IntegTestHelper {

  test("Parquet File Import test") {
    launch("water.sparkling.itest.standalone.ParquetImportTest",
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

object ParquetImportTest {

  def test(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetImportTest")

    // Launch H2O
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._

    implicit val sqlContext = new SQLContext(sc)

    // Import Parquet file into Spark as DataFrame
    val parquetFile = sqlContext.read.parquet("hdfs://mr-0xd6-precise1.0xdata.loc:8020/datasets/airlines/airlines.parquet")
    parquetFile.registerTempTable("parquetFile")

    // Check Parquet file copies correctly
    val AllFlights: H2OFrame = parquetFile
    assert(AllFlights.numRows == parquetFile.count, "Transfer of H2ORDD to SparkRDD completed!")

    // Filter SchemaRdd and push to H2O as H2O H2OFrame
    val ORDFlights = parquetFile.filter("Dest == ORD")
    assert(ORDFlights.count == 313943, "Correctly filtered out all ORD flights!")

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

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}