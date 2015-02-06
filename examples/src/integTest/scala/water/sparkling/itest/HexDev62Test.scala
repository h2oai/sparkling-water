package water.sparkling.itest

import hex.kmeans.KMeans
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.examples.h2o.AirlinesParse
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.junit.JUnitRunner
import water.{DKV, Key}
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class HexDev62TestSuite extends FunSuite {

  val swassembly = sys.props.getOrElse("sparkling.test.assembly",
    fail("The variable 'sparkling.test.assembly' is not set! It should point to assembly jar file."))

  val testJar = sys.props.getOrElse("sparkling.test.jar", fail("The variable 'sparkling.test.jar' should "))

  test("Transfer of Spark SchemaRDD to H2O Data Frame") {
    // Requests environment:
    // - MASTER=YARN-CLIENT
    // - SPARK_HOME=spark-1.2.0-hadoop2.4
    //
    val cmdLine = Array[String]("--class", "water.sparkling.itest.HexDev62Test", "--jars", swassembly, testJar)
    println(cmdLine.mkString(","))
    SparkSubmit.main(cmdLine)
  }
}

object HexDev62Test {
  def main(args: Array[String]): Unit = {
    val swassembly = sys.props.getOrElse("sparkling.test.assembly",
      throw new IllegalArgumentException("The variable 'sparkling.test.assembly' is not set! It should point to assembly jar file."))
    val conf = new SparkConf().setAppName("HexDev62TestSuite").setJars(swassembly :: Nil)
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext._
    // Import all year airlines into SPARK
    // val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
    val path = "/Users/amy/0xdata/data/airlines_all.05p.csv"
    val timer1 = new water.util.Timer
    val airlinesRaw = sc.textFile(path)

    val airlinesRDD = airlinesRaw.map(_.split(",")).map(row => AirlinesParse(row)).filter(!_.isWrongRow())
    
    // Convert RDD to H2O Frame
    val airlinesData : DataFrame = airlinesRDD
    
    // Convert RDD to SchemaRDD
    val schemaString = airlinesRDD.first
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = airlinesRDD.map(_.split(",")).map(p => Row(p(0), p(1), p(2), p(3), p(4), 
      p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), 
      p(18), p(19), p(20), p(21), p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30)))

    val airlinesSchemaRDD = sqlContext.applySchema(rowRDD, schema)
    val airlinesDataFrame : DataFrame = airlinesSchemaRDD


    case class Record(i:Int, v:String)
    val rdd = sc.parallelize(airlinesRDD.map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    rdd.registerTempTable("records")
    val results: SchemaRDD = sql("SELECT * FROM records")
    
    // Convert SchemaRDD to H2O Data Frame

    val timeToParse = timer1.time
//
//    // Transfer data from H2O to Spark RDD
//
//    import org.apache.spark.sql.SQLContext
//
//    val timer2 = new water.util.Timer
//    implicit val sqlContext = new SQLContext(sc)
//    val airlinesRDD = asSchemaRDD(airlinesData)(sqlContext)
//    val timeToTransfer = timer2.time
//
//    assert (airlinesData.numRows == airlinesRDD.count, "Transfer of H2ORDD to SparkRDD completed!")
//
//    // Run Kmeans in Spark  on indices 10,19,26 (FlightNo, Distance, WeatherDelay)
//
//    val airlinesVectorRDD = airlinesRDD.map(row => Vectors.dense(row.getByte(1) * 1.0, row.getByte(2) * 1.0, row.getByte(3) * 1.0))
//
//    val timer3 = new water.util.Timer
//    val clusters = KMeans.train(airlinesVectorRDD, 5, 20)
//    val timeForKMModel = timer3.time
//
//    // Evaluate clustering by computing within set sum of squared errors
//    val WSSSE = clusters.computeCost(airlinesVectorRDD)
//    println("Within Set Sum of Squared Errors = " + WSSSE)
  }
}
