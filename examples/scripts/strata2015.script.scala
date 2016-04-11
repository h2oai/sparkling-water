/**
 * Launch following commands:
 *   export MASTER="local-cluster[3,2,4096]"
 *   bin/sparkling-shell -i examples/scripts/strata2015.script.scala
 *
  * When running using spark shell or using scala rest API:
  *    SQLContext is available as sqlContext
  *     - if you want to use sqlContext implicitly, you have to redefine it like: implicit val sqlContext = sqlContext,
  *      but better is to use it like this: implicit val sqlContext = SQLContext.getOrCreate(sc)
  *    SparkContext is available as sc
  */
// Common imports
import org.apache.spark.SparkFiles
import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.sql.SQLContext
import hex.tree.gbm.GBM
import hex.tree.gbm.GBMModel.GBMParameters
import water.app.ModelMetricsSupport

// Create SQL support
implicit val sqlContext = SQLContext.getOrCreate(sc)
import sqlContext.implicits._

// Start H2O services
implicit val h2oContext = H2OContext.getOrCreate(sc)
import h2oContext._
import h2oContext.implicits._

val location = "examples/bigdata/laptop/citibike-nyc/"
val fileNames = Seq[String]("2013-07.csv","2013-08.csv","2013-09.csv","2013-10.csv","2013-11.csv","2013-12.csv")
val filesPaths = fileNames.map(name => location + name) :+ location+"31081_New_York_City__Hourly_2013.csv"

// Register files to SparkContext
addFiles(sc, filesPaths:_*)

// Load and parse data into H2O
val dataFiles = fileNames.map(name => new java.io.File(SparkFiles.get(name)).toURI)
val bikesDF = new H2OFrame(dataFiles:_*)
// Rename columns and remove all spaces in header
val colNames = bikesDF.names().map( n => n.replace(' ', '_'))
bikesDF._names = colNames
bikesDF.update()

//
// Transform start time to days from Epoch
//
// Select column 'startime'
val startTimeF = bikesDF('starttime)
// Add a new column
bikesDF.add(new TimeSplit().doIt(startTimeF))
// Do not forget to update frame in K/V store
bikesDF.update()

//
// Transform H2OFrame into DataFrame
//
val bikesRdd = asDataFrame(bikesDF)

// Register table and SQL table
bikesRdd.registerTempTable("bikesRdd")

//
// Do grouping with help of Spark SQL
//
val bikesPerDayRdd = sqlContext.sql(
  """SELECT Days, start_station_id, count(*) bikes
    |FROM bikesRdd
    |GROUP BY Days, start_station_id """.stripMargin)

//
// Convert RDD to H2OFrame via implicit operation
//
val bikesPerDayDF:H2OFrame = bikesPerDayRdd

//
// Perform time transformation
//
// Select "Days" column
val daysVec = bikesPerDayDF('Days)
// Run transformation TimeTransform
val finalBikeDF = bikesPerDayDF.add(new TimeTransform().doIt(daysVec))

//
// Define function to build a model
//
def buildModel(df: H2OFrame, trees: Int = 100, depth: Int = 6)(implicit h2oContext: H2OContext):R2 = {
    import h2oContext.implicits._
    // Split into train and test parts
    val frs = splitFrame(df, Seq("train.hex", "test.hex", "hold.hex"), Seq(0.6, 0.3, 0.1))
    val (train, test, hold) = (frs(0), frs(1), frs(2))
    // Configure GBM parameters
    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._valid = test
    gbmParams._response_column = 'bikes
    gbmParams._ntrees = trees
    gbmParams._max_depth = depth
    // Build a model
    val gbmModel = new GBM(gbmParams).trainModel.get
    // Score datasets
    Seq(train,test,hold).foreach(gbmModel.score(_).delete)
    // Collect R2 metrics
    val result = R2("Model #1", ModelMetricsSupport.r2(gbmModel, train), r2(gbmModel, test), r2(gbmModel, hold))
    // Perform clean-up
    Seq(train, test, hold).foreach(_.delete())
    result
}

//
// Build a model
//
val result1 = buildModel(finalBikeDF)

// Load weather data in NY 2013
val weatherData = sc.textFile(SparkFiles.get("31081_New_York_City__Hourly_2013.csv"))
// Parse data and filter them
val weatherRdd = weatherData.map(_.split(",")).
  map(row => NYWeatherParse(row)).
  filter(!_.isWrongRow()).
  filter(_.HourLocal == Some(12)).setName("weather").cache()


// Join with bike table
weatherRdd.toDF.registerTempTable("weatherRdd")
asDataFrame(finalBikeDF).registerTempTable("bikesRdd")

val bikesWeatherRdd = sqlContext.sql(
    """SELECT b.Days, b.start_station_id, b.bikes,
      |b.Month, b.DayOfWeek,
      |w.DewPoint, w.HumidityFraction, w.Prcp1Hour,
      |w.Temperature, w.WeatherCode1
      | FROM bikesRdd b
      | JOIN weatherRdd w
      | ON b.Days = w.Days
    """.stripMargin)

// And make prediction again but now on RDD
val result2 = buildModel(bikesWeatherRdd)

// Kill the cloud
//sc.stop()
