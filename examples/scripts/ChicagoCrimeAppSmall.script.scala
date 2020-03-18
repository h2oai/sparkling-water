/**
 * Launch following commands:
 * export MASTER="local[*]"
 * bin/sparkling-shell -i examples/scripts/ChicagoCrimeAppSmall.script.scala --conf spark.executor.memory=3G
 *
 */

import _root_.hex.genmodel.utils.DistributionFamily
import _root_.hex.deeplearning.DeepLearningModel
import _root_.hex.tree.gbm.GBMModel
import _root_.hex.{Model, ModelMetricsBinomial}
import org.apache.spark.h2o._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import water.parser.ParseSetup
import water.support.{H2OFrameSupport, ModelMetricsSupport}
import water.support.H2OFrameSupport._
import org.apache.spark.sql.functions.udf
import water.fvec.Vec
import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import java.io.File
import org.apache.spark.sql.Row

// Start H2O services
val hc = H2OContext.getOrCreate()

//
// H2O Data loader using H2O API
//
def loadData(datafile: String, modifyParserSetup: ParseSetup => ParseSetup = identity[ParseSetup]): H2OFrame = {
  val uri = java.net.URI.create(datafile)
  val parseSetup = modifyParserSetup(water.fvec.H2OFrame.parserSetup(uri))
  new H2OFrame(parseSetup, new java.net.URI(datafile))
}

def createWeatherTable(datafile: String): DataFrame = {
  val table = loadData(datafile)
  val fr = withLockAndUpdate(table) {
    // Remove first column since we do not need it
    _.remove(0).remove()
  }
  hc.asDataFrame(fr)
}

def createCensusTable(datafile: String): DataFrame = {
  val table = loadData(datafile)
  val fr = withLockAndUpdate(table) { fr =>
    val colNames = fr.names().map(n => n.trim.replace(' ', '_').replace('+', '_'))
    fr._names = colNames
  }
  hc.asDataFrame(fr)
}

def SEASONS: Array[String] = Array[String]("Spring", "Summer", "Autumn", "Winter")

def getSeason(month: Int): String = {
  val seasonNum =
    if (month >= 3 && month <= 5) 0 // Spring
    else if (month >= 6 && month <= 8) 1 // Summer
    else if (month >= 9 && month <= 10) 2 // Autumn
    else 3 // Winter
  SEASONS(seasonNum)
}

def isWeekend(dayOfWeek: Int): Int = if (dayOfWeek == 7 || dayOfWeek == 6) 1 else 0

val seasonUdf = udf(getSeason _)
val weekendUdf = udf(isWeekend _)

def addAdditionalDateColumns(df: DataFrame): DataFrame = {
  import spark.implicits._
  import org.apache.spark.sql.functions._
  df
    .withColumn("Date", from_unixtime(unix_timestamp('Date, "MM/dd/yyyy hh:mm:ss a")))
    .withColumn("Year", year('Date))
    .withColumn("Month", month('Date))
    .withColumn("Day", dayofmonth('Date))
    .withColumn("WeekNum", weekofyear('Date))
    .withColumn("HourOfDay", hour('Date))
    .withColumn("Season", seasonUdf('Month))
    .withColumn("WeekDay", date_format('Date, "u"))
    .withColumn("Weekend", weekendUdf('WeekDay))
    .drop('Date)
}

def createCrimeTable(datafile: String): DataFrame = {
  val table = loadData(datafile, (parseSetup: ParseSetup) => {
    val colNames = parseSetup.getColumnNames
    val typeNames = parseSetup.getColumnTypes
    colNames.indices.foreach { idx =>
      if (colNames(idx) == "Date") typeNames(idx) = Vec.T_STR
    }
    parseSetup
  })
  val fr = withLockAndUpdate(table) { fr =>
    val colNames = fr.names().map(n => n.trim.replace(' ', '_'))
    fr._names = colNames
  }

  val sparkFrame = hc.asDataFrame(fr)
  addAdditionalDateColumns(sparkFrame)
}

val weatherFile = new File("./examples/smalldata/chicago/chicagoAllWeather.csv").getAbsolutePath
val censusFile = new File("./examples/smalldata/chicago/chicagoCensus.csv").getAbsolutePath
val crimesFile = new File("./examples/smalldata/chicago/chicagoCrimes10k.csv.zip").getAbsolutePath

val weatherTable = createWeatherTable(weatherFile)
weatherTable.createOrReplaceTempView("chicagoWeather")
// Census data
val censusTable = createCensusTable(censusFile)
censusTable.createOrReplaceTempView("chicagoCensus")
// Crime data
val crimeTable = createCrimeTable(crimesFile)
crimeTable.createOrReplaceTempView("chicagoCrime")

//
// Join crime data with weather and census tables
//
val crimeWeather = spark.sql(
  """SELECT
    |a.Year, a.Month, a.Day, a.WeekNum, a.HourOfDay, a.Weekend, a.Season, a.WeekDay,
    |a.IUCR, a.Primary_Type, a.Location_Description, a.Community_Area, a.District,
    |a.Arrest, a.Domestic, a.Beat, a.Ward, a.FBI_Code,
    |b.minTemp, b.maxTemp, b.meanTemp,
    |c.PERCENT_AGED_UNDER_18_OR_OVER_64, c.PER_CAPITA_INCOME, c.HARDSHIP_INDEX,
    |c.PERCENT_OF_HOUSING_CROWDED, c.PERCENT_HOUSEHOLDS_BELOW_POVERTY,
    |c.PERCENT_AGED_16__UNEMPLOYED, c.PERCENT_AGED_25__WITHOUT_HIGH_SCHOOL_DIPLOMA
    |FROM chicagoCrime a
    |JOIN chicagoWeather b
    |ON a.Year = b.year AND a.Month = b.month AND a.Day = b.day
    |JOIN chicagoCensus c
    |ON a.Community_Area = c.Community_Area_Number""".stripMargin)

//
// Publish as H2O Frame
crimeWeather.printSchema()
val crimeWeatherDF = hc.asH2OFrame(crimeWeather)
// Transform all string columns into categorical
allStringVecToCategorical(crimeWeatherDF)


//
// Split final data table
//
val keys = Array[String]("train.hex", "test.hex")
val ratios = Array[Double](0.8, 0.2)
val frs = splitFrame(crimeWeatherDF, keys, ratios)
val (train, test) = (hc.asH2OFrame(frs(0)), hc.asH2OFrame(frs(1)))

//
// Show results
//

def GBMModel(train: H2OFrame, test: H2OFrame, response: String,
             ntrees: Int = 10, depth: Int = 6, family: DistributionFamily = DistributionFamily.bernoulli): GBMModel = {
  import _root_.hex.tree.gbm.GBM
  import _root_.hex.tree.gbm.GBMModel.GBMParameters

  val gbmParams = new GBMParameters()
  gbmParams._train = train.key
  gbmParams._valid = test.key
  gbmParams._response_column = response
  gbmParams._ntrees = ntrees
  gbmParams._max_depth = depth
  gbmParams._distribution = family

  val gbm = new GBM(gbmParams)
  val model = gbm.trainModel.get
  model
}

def DLModel(train: H2OFrame, test: H2OFrame, response: String,
            epochs: Int = 10, l1: Double = 0.0001, l2: Double = 0.0001,
            activation: Activation = Activation.RectifierWithDropout, hidden: Array[Int] = Array(200, 200)): DeepLearningModel = {
  import _root_.hex.deeplearning.DeepLearning

  val dlParams = new DeepLearningParameters()
  dlParams._train = train.key
  dlParams._valid = test.key
  dlParams._response_column = response
  dlParams._epochs = epochs
  dlParams._l1 = l1
  dlParams._l2 = l2
  dlParams._activation = activation
  dlParams._hidden = hidden

  // Create a job
  val dl = new DeepLearning(dlParams)
  val model = dl.trainModel.get
  model
}

//
// Build GBM model
//
val gbmModel = GBMModel(train, test, "Arrest")

//
// Build Deep Learning model
//
val dlModel = DLModel(train, test, "Arrest")

// Collect model metrics
def binomialMetrics[M <: Model[M, P, O], P <: _root_.hex.Model.Parameters, O <: _root_.hex.Model.Output]
(model: Model[M, P, O], train: H2OFrame, test: H2OFrame): (ModelMetricsBinomial, ModelMetricsBinomial) = {
  import water.support.ModelMetricsSupport._
  (modelMetrics(model, train), modelMetrics(model, test))
}

val (trainMetricsGBM, testMetricsGBM) = binomialMetrics(gbmModel, train, test)
val (trainMetricsDL, testMetricsDL) = binomialMetrics(dlModel, train, test)

//
// Print Scores of GBM & Deep Learning
//
println(
  s"""Model performance:
     |  GBM:
     |    train AUC = ${trainMetricsGBM.auc}
     |    test  AUC = ${testMetricsGBM.auc}
     |  DL:
     |    train AUC = ${trainMetricsDL.auc}
     |    test  AUC = ${testMetricsDL.auc}
      """.stripMargin)

//
// Create a predictor
//
def scoreEvent(crime: Row, model: Model[_, _, _], censusTable: DataFrame): Float = {
  // Create Spark DataFrame from a single row
  val schema = StructType(Array(
    StructField("Date", StringType, true),
    StructField("IUCR", ShortType, true),
    StructField("Primary_Type", StringType, true),
    StructField("Location_Description", StringType, true),
    StructField("Domestic", BooleanType, true),
    StructField("Beat", ShortType, true),
    StructField("District", ByteType, true),
    StructField("District", ByteType, true),
    StructField("Community_Area", ByteType, true),
    StructField("FBI_Code", ByteType, true)))

  val rdd = spark.sparkContext.parallelize(Seq(crime))
  val df = addAdditionalDateColumns(spark.createDataFrame(rdd, schema)).withColumn("Domestic", 'Domestic.cast(StringType))
  // Join table with census data
  val row: H2OFrame = hc.asH2OFrame(censusTable.join(df).where('Community_Area === 'Community_Area_Number))
  // Transform all string columns into categorical
  allStringVecToCategorical(row)

  val predictTable = model.score(row)
  val probOfArrest = predictTable.vec("true").at(0)

  probOfArrest.toFloat
}

// Score some crimes

// Define crimes
val crimes = Seq(
  Row("02/08/2015 11:43:58 PM", 1811.toShort, "NARCOTICS", "STREET", false, 422.toShort, 4.toByte, 7.toByte, 46.toByte, 18.toByte),
  Row("02/08/2015 11:00:39 PM", 1150.toShort, "DECEPTIVE PRACTICE", "RESIDENCE", false, 923.toShort, 9.toByte, 14.toByte, 63.toByte, 11.toByte))

// Score
crimes.foreach { crime =>
  val arrestProbGBM = 100 * scoreEvent(crime, gbmModel, censusTable)
  val arrestProbDL = 100 * scoreEvent(crime, dlModel, censusTable)
  println(
    s"""
       |Crime: $crime
       |  Probability of arrest best on DeepLearning: $arrestProbDL %
       |  Probability of arrest best on GBM: $arrestProbGBM %
       |
        """.stripMargin)
}

//
// More data munging
//
// Collect all crime types
val allCrimes = spark.sql("SELECT Primary_Type, count(*) FROM chicagoCrime GROUP BY Primary_Type").collect
// Filter only successful arrests
val crimesWithArrest = spark.sql("SELECT Primary_Type, count(*) FROM chicagoCrime WHERE Arrest = 'true' GROUP BY Primary_Type").collect
// Compute scores
val crimeTypeToArrest = collection.mutable.Map[String, Long]()
allCrimes.foreach(c => if (!c.isNullAt(0)) crimeTypeToArrest += (c.getString(0) -> c.getLong(1)))
val numOfAllCrimes = crimeTable.count
val numOfAllArrests = spark.sql("SELECT * FROM chicagoCrime WHERE Arrest = 'true'").count
// Create a table with:
val crimeTypeArrestRate = crimesWithArrest.map(c =>
  if (!c.isNullAt(0)) {
    val crimeType = c.getString(0)
    val count: Long = crimeTypeToArrest.get(crimeType).getOrElse(0)
    Row(crimeType, c.getLong(1).toDouble / count, c.getLong(1), count, c.getLong(1) / numOfAllArrests.toDouble, c.getLong(1) / count.toDouble, count / numOfAllCrimes.toDouble)
  }).map(_.asInstanceOf[Row])
val schema = StructType(Seq(
  StructField("CrimeType", StringType, false),
  StructField("ArrestRate", DoubleType, false),
  StructField("NumOfArrests", LongType, false),
  StructField("NumOfCrimes", LongType, false),
  StructField("ArrestsToAllArrests", DoubleType, false),
  StructField("ArrestsToAllCrimes", DoubleType, false),
  StructField("CrimesToAllCrimes", DoubleType, false)))

val rowRdd = spark.sparkContext.parallelize(crimeTypeArrestRate).sortBy(x => -x.getDouble(1))
val rateSRdd = spark.createDataFrame(rowRdd, schema)

// Transfer it into H2O
val rateFrame: H2OFrame = hc.asH2OFrame(rateSRdd, Some("RATES"))

/*
In flow type this:
plot (g) -> g(
  g.rect(
    g.position "CrimeType", "ArrestRate"
)
g.from inspect "data", getFrame "frame_rdd_83"
)

*/

/*
plot (g) -> g(
  g.rect(
    g.position "CrimeType", "ArrestRate"
    g.fillColor g.value 'blue'
    g.fillOpacity g.value 0.75

  )
  g.rect(
    g.position "CrimeType", "CrimesToAllCrimes"
    g.fillColor g.value 'red'
    g.fillOpacity g.value 0.65

  )

  g.from inspect "data", getFrame "frame_rdd_83"
)
*/
