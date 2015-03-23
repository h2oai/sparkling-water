package water.sparkling.itest.standalone

import org.apache.spark.examples.h2o.AirlinesParse
import org.apache.spark.h2o._
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.SparkITest
import java.io.File
import org.apache.spark.sql.SQLContext




/**
 * Test for Parquet Import : Save small airlines data as Parquet File, 
 * import Parquet file into Spark as SchemaRDD, then run Deep Learning
 * using H2O*
 */
@RunWith(classOf[JUnitRunner])
class ParquetImportTestSuite extends FunSuite with SparkITest {

  ignore("Parquet File Import test") {
    launch( "water.sparkling.itest.standalone.ParquetImport",
      env {
        sparkMaster("spark://mr-0xd1-precise1.0xdata.loc:7077")
        // Configure Standalone environment
        conf("spark.standalone.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 8) // 8 executor instances
        conf("spark.executor.memory", "10g") // 10g per executor
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

    // Import airlines file into H2O
    val dataFile = "hdfs://mr-0xd6-precise1.0xdata.loc:8020/datasets/airlines/airlines_all.05p.csv"
    val airlinesData = new DataFrame(new File(dataFile))

    // Save airlines file as Parquet File
    implicit val sqlContext = new SQLContext(sc)
    val airlinesRDD = asSchemaRDD(airlinesData)(sqlContext)
    airlinesRDD.saveAsParquetFile("airlines.parquet")

    // Import Parquet file into Spark as SchemaRDD
    val parquetFile = sqlContext.parquetFile("airlines.parquet")
    parquetFile.registerTempTable("parquetFile")

    // Filter SchemaRdd and push to H2O as H2O DataFrame
    val ORDFlights = parquetFile.filter(r => r(17) == "ORD")
    ORDFlights.count
    ORDFlights.collect

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

    // Clean up Parquet File

    def removeAll(path: String) = {
      def getRecursively(f: File): Seq[File] =
        f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles
      val parent = new File(path)
      getRecursively(parent).foreach { f =>
        if (!f.delete())
          throw new RuntimeException("Failed to delete " + f.getAbsolutePath)
      }
      if (!parent.delete())
        throw new RuntimeException("Failed to delete " + parent.getAbsolutePath)
      else
        println("Finish deleting file/directory : " + path)
    }

    removeAll("airlines.parquet")
    sc.stop()
  }
}

