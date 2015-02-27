//
// Meetup script 2015-02-26
//

// Location of download data
val DIR_PREFIX = "/Users/michal/Devel/projects/h2o/repos/h2o2/bigdata/laptop/citibike-nyc/"
import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import water.fvec._
import hex.tree.gbm.GBMModel
import hex.tree.gbm.GBM
import hex.tree.gbm.GBMModel.GBMParameters

// Initialize SQLConcept
implicit val sqlContext = new SQLContext(sc)
import sqlContext._

// Create H2O context and start H2O services around the Spark cluster
implicit val h2oContext = new H2OContext(sc).start()
import h2oContext._

//
// Load and parse bike data (year 2013) into H2O by using H2O parser
//

val dataFiles = Array[String](
    "2013-07.csv", "2013-08.csv", "2013-09.csv", "2013-10.csv",
    "2013-11.csv", "2013-12.csv").map(f => new java.io.File(DIR_PREFIX, f))

// Load and parse data
val bikesDF = new DataFrame(dataFiles:_*)

// Rename columns and remove all spaces in header
val colNames = bikesDF.names().map( n => n.replace(' ', '_'))
bikesDF._names = colNames
bikesDF.update(null) // Update Frame in DKV

//
// Transform start time to days from Epoch
//
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

def buildModel(df: DataFrame, modelName: String = "GBM model", trees: Int = 200, depth: Int = 6):R2 = {
    import hex.splitframe.ShuffleSplitFrame
    import water.Key
    //
    // Split into train and test parts
    //
    val keys = Array[String]("train.hex", "test.hex", "hold.hex").map(Key.make(_))
    val ratios = Array[Double](0.6, 0.3, 0.1)
    val frs = ShuffleSplitFrame.shuffleSplitFrame(df, keys, ratios, 1234567689L)
    val (train, test, hold) = (frs(0), frs(1), frs(2))

    //
    // Launch GBM prediction
    //
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters
    // Specify parameters
    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._valid = test
    gbmParams._response_column = 'bikes
    gbmParams._ntrees = trees
    gbmParams._max_depth = depth
    // Launch computation
    val gbmModel = new GBM(gbmParams).trainModel.get

    // Run scoring (i like Scala!)
    Seq(train,test,hold).foreach(gbmModel.score(_).delete)

    // Collect R2 metrics
    val result = R2(s"Model GBM($modelName, trees: $trees, depth: $depth)", r2(gbmModel, train), r2(gbmModel, test), r2(gbmModel, hold))
    // Perform clean-up
    Seq(train, test, hold).foreach(_.delete())
    result
}

// Build model
val result1 = buildModel(finalBikeDF, "bike data")

// Grid search over different parameters
// Increase number of trees, but decrease trees depth
val gridSearch = Seq( (10, 8), (100, 4), (200,2))
val gridSearchResult1 = gridSearch.map( x => buildModel(finalBikeDF, "bike data", trees = x._1, depth = x._2) ) ++ Seq(result1)

// Load weather data in NY 2013
val weatherData = sc.textFile(DIR_PREFIX + "31081_New_York_City__Hourly_2013.csv")
// Parse data and filter them
val weatherRdd = weatherData.map(_.split(",")).
  map(row => NYWeatherParse(row)).
  filter(!_.isWrongRow()).
  filter(_.HourLocal == Some(12)).cache()


// Join with bike table
sqlContext.registerRDDAsTable(weatherRdd, "weatherRdd")
sqlContext.registerRDDAsTable(asSchemaRDD(finalBikeDF), "bikesRdd")

val bikesWeatherRdd = sql(
    """SELECT b.Days, b.start_station_id, b.bikes, b.Month, b.DayOfWeek,
      |w.DewPoint, w.HumidityFraction, w.Prcp1Hour, w.Temperature, w.WeatherCode1
      | FROM bikesRdd b
      | JOIN weatherRdd w
      | ON b.Days = w.Days
      |
    """.stripMargin)

// And make prediction again but now on RDD
val result2 = buildModel(bikesWeatherRdd)
val gridSearchResult2 = gridSearch.map( x => buildModel(bikesWeatherRdd, "bike+weather data", trees = x._1, depth = x._2) ) ++ Seq(result2)

// Kill the cloud
sc.stop()
