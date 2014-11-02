package org.apache.spark.examples.h2o

import java.io.File

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.{SparkFiles, SparkConf, SparkContext}
import org.apache.spark.h2o.{DoubleHolder, H2OContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import water.fvec.DataFrame
import DemoUtils.configure


object AirlinesWithWeatherDemo {

  def main(args: Array[String]): Unit = {
    // Configure this application
    val conf: SparkConf = configure("Sparkling Water: Join of Airlines with Weather Data")

    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._
    // Setup environment
    sc.addFile("examples/smalldata/Chicago_Ohare_International_Airport.csv")
    sc.addFile("examples/smalldata/allyears2k_headers.csv.gz")

    //val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
    val wrawdata = sc.textFile(SparkFiles.get("Chicago_Ohare_International_Airport.csv"),3).cache()
    val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())

    //
    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val airlinesData = new DataFrame(new File(SparkFiles.get("allyears2k_headers.csv.gz")))

    val airlinesTable : RDD[Airlines] = toRDD[Airlines](airlinesData)
    // Select flights only to ORD
    val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))

    flightsToORD.count
    println(s"\nFlights to ORD: ${flightsToORD.count}\n")

    val sqlContext = new SQLContext(sc)
    import sqlContext._ // import implicit conversions
    flightsToORD.registerTempTable("FlightsToORD")
    weatherTable.registerTempTable("WeatherORD")

    val bigTable = sql(
      """SELECT
        |f.Year,f.Month,f.DayofMonth,
        |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
        |f.UniqueCarrier,f.FlightNum,f.TailNum,
        |f.Origin,f.Distance,
        |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
        |f.ArrDelay
        |FROM FlightsToORD f
        |JOIN WeatherORD w
        |ON f.Year=w.Year AND f.Month=w.Month AND f.DayofMonth=w.Day""".stripMargin)
    println(s"\nResult of query: ${bigTable.count}\n")
    bigTable.take(10).foreach(println(_))

    //
    // -- Run DeepLearning
    //
    val dlParams = new DeepLearningParameters()
    dlParams._train = bigTable
    dlParams._response_column = 'ArrDelay
    dlParams._classification = false
    dlParams.epochs = 100

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.train.get

    val predictionH2OFrame = dlModel.score(bigTable)('predict)
    val predictionsFromModel = toRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    println(
      s"""# R script for residual plot
        |h = h2o.init()
        |
        |pred = h2o.getFrame(h, "${predictionH2OFrame._key}")
        |act = h2o.getFrame (h, "${bigTable._key}")
        |
        |predDelay = pred$$predict
        |actDelay = act$$ArrDelay
        |
        |nrow(actDelay) == nrow(predDelay)
        |
        |residuals = predDelay - actDelay
        |
        |compare = cbind (as.data.frame(actDelay$$ArrDelay), as.data.frame(residuals$$predict))
        |nrow(compare)
        |plot( compare[,1:2] )
        |
      """.stripMargin)
    // Explicit sleep for long time to make cluster available from R
    Thread.sleep(60*60*1000)
    sc.stop()
  }
}
