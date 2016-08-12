package water.sparkling.itest.standalone

import hex.Distribution
import org.apache.spark.h2o.{DoubleHolder, H2OContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.H2OFrame
import water.sparkling.itest.standalone.ParquetImportTest._
import water.sparkling.itest.{IntegTestStopper, IntegTestHelper}


/**
  * Test for Jira Hex-Dev 100 : Import airlines data and run a host of classification models,
  * including GBM, GLM, and Deep Learning.
  */
@RunWith(classOf[JUnitRunner])
class HexDev100TestSuite extends FunSuite with IntegTestHelper {

  ignore("HEX-DEV 100 test") {
    launch("water.sparkling.itest.standalone.HexDev100Test",
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

object HexDev100Test extends IntegTestStopper{

  def main(args: Array[String]): Unit = exitOnException{
    val conf = new SparkConf().setAppName("HexDev100Test")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._

    // Import all year airlines into H2O
    val path = "hdfs://mr-0xd6-precise1.0xdata.loc:8020/datasets/airlines/airlines_all.csv"
    val uri = new java.net.URI(path)
    val airlinesData = new H2OFrame(uri)

    // Pass into Spark to drop unused columns
    implicit val sqlContext = new SQLContext(sc)
    val airlinesDataFrame = asDataFrame(airlinesData)
    airlinesDataFrame.registerTempTable("AirlinesDataTable")
    // Drop all columns except "Year", "Month", "DayOfWeek", "Origin", "Dest", "UniqueCarrier", "Distance", "FlightNum", "IsDepDelayed"
    val airlinesTable = sqlContext.sql(
      """SELECT
        |f.Year, f.Month, f.DayOfWeek,
        |f.Origin, f.Dest, f.UniqueCarrier,
        |f.Distance, f.FlightNum, f.IsDepDelayed
        |FROM AirlinesDataTable f""".stripMargin)


    // Run deep learning to produce model classifying delayed flights
    import hex.deeplearning.DeepLearning
    import hex.deeplearning.DeepLearningModel.DeepLearningParameters
    val dlParams = new DeepLearningParameters()
    dlParams._epochs = 10
    dlParams._train = airlinesTable
    dlParams._response_column = 'IsDepDelayed
    // Create a job
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    // Use model to estimate delay on training data
    val predDLH2OFrame = dlModel.score(airlinesTable)('predict)
    val predDLFromModel = asRDD[DoubleHolder](predDLH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

    // Run GLM to produce model classifying delayed flights
    import hex.glm.GLM
    import hex.glm.GLMModel.GLMParameters
    import hex.glm.GLMModel.GLMParameters.Family
    val glmParams = new GLMParameters(Family.binomial)
    glmParams._train = airlinesTable
    glmParams._response_column = 'IsDepDelayed
    glmParams._alpha = Array[Double](0.5)
    val glm = new GLM(glmParams)
    val glmModel = glm.trainModel().get()

    // Use model to estimate delay on training data
    val predGLMH2OFrame = glmModel.score(airlinesTable)('predict)
    val predGLMFromModel = asRDD[DoubleHolder](predGLMH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

    // Use GBM to produce model classifying delayed flights
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

    val gbmParams = new GBMParameters()
    gbmParams._train = airlinesTable
    gbmParams._response_column = 'IsDepDelayed
    gbmParams._ntrees = 10
    gbmParams._distribution = Distribution.Family.bernoulli

    val gbm = new GBM(gbmParams)
    val gbmModel = gbm.trainModel.get

    // Use model to estimate delay on training data
    val predGBMH2OFrame = glmModel.score(airlinesTable)('predict)
    val predGBMFromModel = asRDD[DoubleHolder](predGBMH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

    println("""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""")
    println("Finished running GLM,GBM, and Deep Learning on airlines dataset.")
    println("""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""")

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
