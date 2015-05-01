//
// Strata script 2015-02-19
//
// Run: bin/sparkling-shell
//

val DIR_PREFIX = "/Users/michal/Devel/projects/h2o/repos/h2o2/bigdata/laptop/citibike-nyc/"

// Common imports
import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.sql.SQLContext
import hex.tree.gbm.GBM
import hex.tree.gbm.GBMModel.GBMParameters

// Initialize Spark SQLContext
implicit val sqlContext = new SQLContext(sc)
import sqlContext._


// Create H2OContext to execute H2O code on Spark cluster
implicit val h2oContext = new H2OContext(sc).start()
import h2oContext._


//
// Load data into H2O by using H2O parser - use only 2013 data.
//

val dataFiles = Array[String](
      "2013-07.csv", "2013-08.csv", "2013-09.csv", "2013-10.csv",
      "2013-11.csv", "2013-12.csv").map(f => new java.io.File(DIR_PREFIX, f).toURI)
// Load and parse data
val bikesDF = new DataFrame(dataFiles:_*)
// Rename columns and remove all spaces in header
val colNames = bikesDF.names().map( n => n.replace(' ', '_'))
bikesDF._names = colNames
bikesDF.update(null)

//
// Transform start time to days from Epoch
//
// Select column 'startime'
val startTimeF = bikesDF('starttime)
// Add a new column
bikesDF.add(new TimeSplit().doIt(startTimeF))
// Do not forget to update frame in K/V store
bikesDF.update(null)

//
// Transform DataFrame into SchemaRDD
//
val bikesRdd = asSchemaRDD(bikesDF)

// Register table and SQL table
sqlContext.registerRDDAsTable(bikesRdd, "bikesRdd")

//
// Do grouping with help of Spark SQL
//
val bikesPerDayRdd = sql(
  """SELECT Days, start_station_id, count(*) bikes
    |FROM bikesRdd
    |GROUP BY Days, start_station_id """.stripMargin)

//
// Convert RDD to DataFrame via implicit operation
//
val bikesPerDayDF:DataFrame = bikesPerDayRdd

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
def buildModel(df: DataFrame, trees: Int = 100, depth: Int = 6):R2 = {
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
    val result = R2("Model #1", r2(gbmModel, train), r2(gbmModel, test), r2(gbmModel, hold))
    // Perform clean-up
    Seq(train, test, hold).foreach(_.delete())
    result
}

//
// Build a model
//
val result1 = buildModel(finalBikeDF)

// Load weather data in NY 2013
val weatherData = sc.textFile(DIR_PREFIX + "31081_New_York_City__Hourly_2013.csv")
// Parse data and filter them
val weatherRdd = weatherData.map(_.split(",")).
  map(row => NYWeatherParse(row)).
  filter(!_.isWrongRow()).
  filter(_.HourLocal == Some(12)).setName("weather").cache()


// Join with bike table
sqlContext.registerRDDAsTable(weatherRdd, "weatherRdd")
sqlContext.registerRDDAsTable(asSchemaRDD(finalBikeDF), "bikesRdd")

val bikesWeatherRdd = sql(
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