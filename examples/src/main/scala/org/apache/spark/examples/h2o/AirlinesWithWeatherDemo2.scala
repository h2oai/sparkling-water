package org.apache.spark.examples.h2o

import java.io.File

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import hex.SplitFrame
import hex.tree.gbm.GBM
import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.spark.examples.h2o.DemoUtils.{addFiles, configure, residualPlotRCode}
import org.apache.spark.h2o.{DoubleHolder, H2OContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import water.fvec.DataFrame

/** Demo for meetup presented at 12/17/2014 */
object AirlinesWithWeatherDemo2 {

  def main(args: Array[String]): Unit = {
    // Configure this application
    val conf: SparkConf = configure("Sparkling Water Meetup: Use Airlines and Weather Data for delay prediction")
    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._
    // Setup environment
    addFiles(sc,
      "examples/smalldata/Chicago_Ohare_International_Airport.csv",
      "examples/smalldata/year2005.csv.gz")

    //val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
    val wrawdata = sc.textFile(SparkFiles.get("Chicago_Ohare_International_Airport.csv"),3).cache()
    val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())

    //
    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val airlinesData = new DataFrame(new File(SparkFiles.get("year2005.csv.gz")))

    val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
    // Select flights only to ORD
    val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))

    flightsToORD.count
    println(s"\nFlights to ORD: ${flightsToORD.count}\n")

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext._ // import implicit conversions
    flightsToORD.registerTempTable("FlightsToORD")
    weatherTable.registerTempTable("WeatherORD")

    //
    // -- Join both tables and select interesting columns
    //
    val joinedTable = sql(
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
    println(s"\nResult of query: ${joinedTable.count}\n")
    //bigTable.collect().foreach(println(_))

    //
    // Split data into 3 tables - train/validation/test
    //
    // Instead of using RDD API we will directly split H2O Frame
    val joinedDataFrame:DataFrame = joinedTable // Invoke implicit transformation
    // Transform date related columns to enums
    for( i <- 0 to 2) joinedDataFrame.replace(i, joinedDataFrame.vec(i).toEnum)

    val sf = new SplitFrame()
    sf.dataset = joinedDataFrame
    sf.ratios = Array(0.7, 0.2)
    sf.execImpl()
    val splits = sf.get().destKeys

    val trainTable = new DataFrame(splits(0))
    val validTable = new DataFrame(splits(1))
    val testTable  = new DataFrame(splits(2))

    //
    // -- Run DeepLearning
    //
    val dlParams = new DeepLearningParameters()
    dlParams._train = trainTable
    dlParams._response_column = 'ArrDelay
    dlParams._valid = validTable
    dlParams._epochs = 5
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](100, 100)
    dlParams._reproducible = true
    dlParams._force_load_balance = false

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    val dlPredictTable = dlModel.score(testTable)('predict)
    val predictionsFromDlModel = asSchemaRDD(dlPredictTable).collect
                                .map(row => if (row.isNullAt(0)) Double.NaN else row(0))

    println(predictionsFromDlModel.length)
    println(predictionsFromDlModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    printf( residualPlotRCode(dlPredictTable, 'predict, testTable, 'ArrDelay) )

    // GBM Model
    val gbmParams = new GBMParameters()
    gbmParams._train = trainTable
    gbmParams._response_column = 'ArrDelay
    gbmParams._valid = validTable
    gbmParams._ntrees = 100
    //gbmParams._learn_rate = 0.01f
    val gbm = new GBM(gbmParams)
    val gbmModel = gbm.trainModel.get

    // Print R code for residual plot
    val gbmPredictTable = gbmModel.score(testTable)('predict)
    printf( residualPlotRCode(gbmPredictTable, 'predict, testTable, 'ArrDelay) )

    sc.stop()
  }
}
