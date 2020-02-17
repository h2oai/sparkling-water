/**
 * Launch following commands:
 * export MASTER="local[*]"
 * bin/sparkling-shell -i examples/scripts/CityBikeSharing.script.scala
 *
 */

import java.io.File

import org.apache.spark.SparkFiles
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import _root_.hex.tree.gbm.GBM
import _root_.hex.tree.gbm.GBMModel.GBMParameters
import _root_.hex.ModelMetricsSupervised
import java.net.URI
import water.support.{H2OFrameSupport, ModelMetricsSupport, SparkContextSupport}
import ai.h2o.sparkling.examples._

// Start H2O services
val hc = H2OContext.getOrCreate(spark)

def locateData(name: String): URI = {
  val abs = new File("/home/0xdiag/bigdata/laptop/citibike-nyc/" + name)
  if (abs.exists()) {
    abs.toURI
  } else {
    new File("./examples/bigdata/laptop/citibike-nyc/" + name).toURI
  }
}

val fileNames = Array[String]("2013-07.csv", "2013-08.csv", "2013-09.csv", "2013-10.csv", "2013-11.csv", "2013-12.csv")

// Load and parse data into H2O
val bikesH2OFrame = new H2OFrame(fileNames.map(name => locateData(name)): _*)

//
// Transform H2OFrame into Spark's DataFrame
//
var bikesSparkFrame = hc.asDataFrame(bikesH2OFrame)

// Remove spaces in column names
for (col <- bikesSparkFrame.columns) {
  bikesSparkFrame = bikesSparkFrame.withColumnRenamed(col, col.replaceAll("\\s", "_"))
}

//
// Transform start time to days from Epoch
//

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

def numberOfDaysSinceEpoch(timestamp: Long): Long = timestamp / (60 * 60 * 24)
val numberOfDaysSinceEpochUdf = udf(numberOfDaysSinceEpoch _)

bikesSparkFrame = bikesSparkFrame.withColumn("Days", numberOfDaysSinceEpochUdf(unix_timestamp('starttime)))

// Register frame as SQL table
bikesSparkFrame.createOrReplaceTempView("bikesTable")

//
// Do grouping with help of Spark SQL
//
val bikesPerDay = spark.sql(
  """SELECT Days, start_station_id, count(*) bikes
    |FROM bikesTable
    |GROUP BY Days, start_station_id """.stripMargin)


//
// Perform time transformation
//
def daysToTimestamp(days: Int): String = (days * (60 * 60 * 24)).toString
val daysToTimestampUdf = udf(daysToTimestamp _)

val bikesPerDayWithDate = bikesPerDay.withColumn("date", from_unixtime(daysToTimestampUdf('Days)))
val bikesPerDayWithMonth = bikesPerDayWithDate.withColumn("Month", month('date))
val bikesPerDayWithMonthDay = bikesPerDayWithMonth.withColumn("DayOfweek", date_format('date, "u"))
val bikesPerDayTransformed = bikesPerDayWithMonthDay.drop('date)
val finalBikesH2OFrame = hc.asH2OFrame(bikesPerDayTransformed)

// Define metrics holder
case class R2(name: String, train: Double, test: Double, hold: Double) {
  override def toString: String =
    s"""
       |Results for $name:
       |  - R2 on train = ${train}
       |  - R2 on test  = ${test}
       |  - R2 on hold  = ${hold}
      """.stripMargin
}

//
// Define function to build a model
//
def buildModel(df: H2OFrame, trees: Int = 100, depth: Int = 6): R2 = {
  // Split into train and test parts
  val frs = H2OFrameSupport.splitFrame(df, Seq("train.hex", "test.hex", "hold.hex"), Seq(0.6, 0.3, 0.1))
  val (train, test, hold) = (frs(0), frs(1), frs(2))
  // Configure GBM parameters
  val gbmParams = new GBMParameters()
  gbmParams._train = train._key
  gbmParams._valid = test._key
  gbmParams._response_column = "bikes"
  gbmParams._ntrees = trees
  gbmParams._max_depth = depth
  // Build a model
  val gbmModel = new GBM(gbmParams).trainModel.get
  // Score datasets
  Seq(train, test, hold).foreach(gbmModel.score(_).delete)
  // Collect R2 metrics
  val result = R2("Model #1",
    ModelMetricsSupport.modelMetrics[ModelMetricsSupervised](gbmModel, train).r2(),
    ModelMetricsSupport.modelMetrics[ModelMetricsSupervised](gbmModel, test).r2(),
    ModelMetricsSupport.modelMetrics[ModelMetricsSupervised](gbmModel, hold).r2())

  // Perform clean-up
  Seq(train, test, hold).foreach(_.delete())
  result
}

//
// Build a model
//
val result1 = buildModel(finalBikesH2OFrame)
SparkContextSupport.addFiles(sc, locateData("31081_New_York_City__Hourly_2013.csv").getPath())
// Load weather data in NY 2013
val weatherData = sc.textFile(SparkFiles.get("31081_New_York_City__Hourly_2013.csv"))
// Parse data and filter them
val weatherRdd = weatherData.map(_.split(",")).
  map(row => NYWeatherParse(row)).
  filter(!_.isWrongRow()).
  filter(_.HourLocal == Some(12)).setName("weather").cache()


// Join with bike table
weatherRdd.toDF.createOrReplaceTempView("weatherRdd")
hc.asDataFrame(finalBikesH2OFrame).createOrReplaceTempView("bikesRdd")

val bikesWeatherRdd = spark.sql(
  """SELECT b.Days, b.start_station_id, b.bikes,
    |b.Month, b.DayOfWeek,
    |w.DewPoint, w.HumidityFraction, w.Prcp1Hour,
    |w.Temperature, w.WeatherCode1
    | FROM bikesRdd b
    | JOIN weatherRdd w
    | ON b.Days = w.Days
    """.stripMargin)

// And make prediction again but now on RDD
val result2 = buildModel(hc.asH2OFrame(bikesWeatherRdd))
