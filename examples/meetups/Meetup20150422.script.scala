/**
 * To start Sparkling Water please type

cd path/to/sparkling/water
export SPARK_HOME="your/spark-1.2.0-installation"
export MASTER="local-cluster[3,2,4096]"

bin/sparkling-shell --conf spark.executor.memory=3G
*/

//
// Prepare environment
//
import hex.deeplearning.DeepLearningModel
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import hex.tree.gbm.GBMModel
import hex.tree.gbm.GBMModel.GBMParameters.Family
import hex.{Model, ModelMetricsBinomial}
import org.apache.spark.SparkFiles
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.examples.h2o.{Crime, RefineDateColumn}
import org.apache.spark.h2o._
import org.apache.spark.sql._
// SQL support
implicit val sqlContext = new SQLContext(sc)
import sqlContext._

//
// Start H2O services
//
implicit val h2oContext = new H2OContext(sc).start()
import h2oContext._


//
// H2O Data loader using H2O API
//
def loadData(datafile: String): DataFrame = new DataFrame(new java.net.URI(datafile))

//
// Loader for weather data
//
def createWeatherTable(datafile: String): DataFrame = {
  val table = loadData(datafile)
  // Remove first column since we do not need it
  table.remove(0).remove()
  table.update(null)
  table
}

//
// Loader for census data
//
def createCensusTable(datafile: String): DataFrame = {
  val table = loadData(datafile)
  // Rename columns: replace ' ' by '_'
  val colNames = table.names().map( n => n.trim.replace(' ', '_').replace('+','_'))
  table._names = colNames
  table.update(null)
  table
}

//
// Load and modify crime data
//
def createCrimeTable(datafile: String, datePattern:String, dateTimeZone:String): DataFrame = {
  val table = loadData(datafile)
  // Refine date into multiple columns
  val dateCol = table.vec(2)
  table.add(new RefineDateColumn(datePattern, dateTimeZone).doIt(dateCol))
  // Update names, replace all ' ' by '_'
  val colNames = table.names().map( n => n.trim.replace(' ', '_'))
  table._names = colNames
  // Remove Date column
  table.remove(2).remove()
  // Update in DKV
  table.update(null)
  table
}

//
// Load data
//
addFiles(sc,
  "examples/smalldata/chicagoAllWeather.csv",
  "examples/smalldata/chicagoCensus.csv",
  "examples/smalldata/chicagoCrimes10k.csv"
)

val weatherTable = asSchemaRDD(createWeatherTable(SparkFiles.get("chicagoAllWeather.csv")))
registerRDDAsTable(weatherTable, "chicagoWeather")
// Census data
val censusTable = asSchemaRDD(createCensusTable(SparkFiles.get("chicagoCensus.csv")))
registerRDDAsTable(censusTable, "chicagoCensus")
// Crime data
val crimeTable  = asSchemaRDD(createCrimeTable(SparkFiles.get("chicagoCrimes10k.csv"), "MM/dd/yyyy hh:mm:ss a", "Etc/UTC"))
registerRDDAsTable(crimeTable, "chicagoCrime")

//
// Join crime data with weather and census tables
//
val crimeWeather = sql(
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
val crimeWeatherDF:DataFrame = crimeWeather

//
// Split final data table
//
import org.apache.spark.examples.h2o.DemoUtils._
val keys = Array[String]("train.hex", "test.hex")
val ratios = Array[Double](0.8, 0.2)
val frs = splitFrame(crimeWeatherDF, keys, ratios)
val (train, test) = (frs(0), frs(1))

//
// Show results
//
openFlow

def GBMModel(train: DataFrame, test: DataFrame, response: String,
             ntrees:Int = 10, depth:Int = 6, loss: Family = Family.bernoulli)
            (implicit h2oContext: H2OContext) : GBMModel = {
  import h2oContext._
  import hex.tree.gbm.GBM
  import hex.tree.gbm.GBMModel.GBMParameters

  val gbmParams = new GBMParameters()
  gbmParams._train = train
  gbmParams._valid = test
  gbmParams._response_column = response
  gbmParams._ntrees = ntrees
  gbmParams._max_depth = depth
  gbmParams._loss = loss

  val gbm = new GBM(gbmParams)
  val model = gbm.trainModel.get
  model
}

def DLModel(train: DataFrame, test: DataFrame, response: String,
            epochs: Int = 10, l1: Double = 0.0001, l2: Double = 0.0001,
            activation: Activation = Activation.RectifierWithDropout, hidden:Array[Int] = Array(200,200))
           (implicit h2oContext: H2OContext) : DeepLearningModel = {
  import h2oContext._
  import hex.deeplearning.DeepLearning
  import hex.deeplearning.DeepLearningModel.DeepLearningParameters

  val dlParams = new DeepLearningParameters()
  dlParams._train = train
  dlParams._valid = test
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
val gbmModel = GBMModel(train, test, 'Arrest)

//
// Build Deep Learning model
//
val dlModel = DLModel(train, test, 'Arrest)

// Collect model metrics
def binomialMetrics[M <: Model[M,P,O], P <: hex.Model.Parameters, O <: hex.Model.Output]
                    (model: Model[M,P,O], train: DataFrame, test: DataFrame):(ModelMetricsBinomial, ModelMetricsBinomial) = {
  model.score(train).delete()
  model.score(test).delete()
  (binomialMM(model,train), binomialMM(model, test))
}

val (trainMetricsGBM, testMetricsGBM) = binomialMetrics(gbmModel, train, test)
val (trainMetricsDL, testMetricsDL) = binomialMetrics(dlModel, train, test)

//
// Print Scores of GBM & Deep Learning
//
println(
  s"""Model performance:
     |  GBM:
     |    train AUC = ${trainMetricsGBM.auc.AUC}
      |    test  AUC = ${testMetricsGBM.auc.AUC}
      |  DL:
      |    train AUC = ${trainMetricsDL.auc.AUC}
      |    test  AUC = ${testMetricsDL.auc.AUC}
      """.stripMargin)

//
// Create a predictor
//
def scoreEvent(crime: Crime, model: Model[_,_,_], censusTable: SchemaRDD)
              (implicit sqlContext: SQLContext, h2oContext: H2OContext): Float = {
  import h2oContext._
  import sqlContext._
  // Create a single row table
  val srdd:SchemaRDD = sqlContext.sparkContext.parallelize(Seq(crime))
  // Join table with census data
  val row: DataFrame = censusTable.join(srdd, on = Option('Community_Area === 'Community_Area_Number)) //.printSchema
  val predictTable = model.score(row)
  val probOfArrest = predictTable.vec("true").at(0)

  probOfArrest.toFloat
}

// Score some crimes

// Define crimes
val crimeExamples = Seq(
  Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET",false, 422, 4, 7, 46, 18),
  Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE",false, 923, 9, 14, 63, 11))

// Score
for (crime <- crimeExamples) {
  val arrestProbGBM = 100 * scoreEvent(crime, gbmModel, censusTable)
  val arrestProbDL = 100 * scoreEvent(crime, dlModel, censusTable)
  println(
    s"""
       |Crime: $crime
        |  Probability of arrest best on DeepLearning: ${arrestProbDL} %
        |  Probability of arrest best on GBM: ${arrestProbGBM} %
        """.stripMargin)
}


//
// More data munging
//
// Collect all crime types
val allCrimes = sql("SELECT Primary_Type, count(*) FROM chicagoCrime GROUP BY Primary_Type").collect
// Filter only successful arrests
val crimesWithArrest = sql("SELECT Primary_Type, count(*) FROM chicagoCrime WHERE Arrest = 'true' GROUP BY Primary_Type").collect
// Compute scores
val crimeTypeToArrest = collection.mutable.Map[String, Long]()
allCrimes.foreach( c => if (!c.isNullAt(0)) crimeTypeToArrest += ( c.getString(0) -> c.getLong(1) ) )
val numOfAllCrimes = crimeTable.count
val numOfAllArrests = sqlContext.sql("SELECT * FROM chicagoCrime WHERE Arrest = 'true'").count
// Create a table with:
val crimeTypeArrestRate = crimesWithArrest.map(c =>
  if (!c.isNullAt(0)) {
    val crimeType = c.getString(0)
    val count:Long = crimeTypeToArrest.get(crimeType).getOrElse(0)
    Row(crimeType, c.getLong(1).toDouble/count, c.getLong(1), count, c.getLong(1)/numOfAllArrests.toDouble, c.getLong(1)/count.toDouble, count/numOfAllCrimes.toDouble) } ).map(_.asInstanceOf[Row])
val schema = StructType(Seq(
  StructField("CrimeType", StringType, false),
  StructField("ArrestRate", DoubleType, false),
  StructField("NumOfArrests", LongType, false),
  StructField("NumOfCrimes", LongType, false),
  StructField("ArrestsToAllArrests", DoubleType, false),
  StructField("ArrestsToAllCrimes", DoubleType, false),
  StructField("CrimesToAllCrimes", DoubleType, false)))
val rowRdd = sc.parallelize(crimeTypeArrestRate).sortBy(x => -x.getDouble(1))
val rateSRdd = sqlContext.applySchema(rowRdd, schema)

import water.fvec.DataFrame; import h2oContext._
// Transfer it into H2O
val rateFrame:DataFrame = rateSRdd

/*
In flow type this:
plot (g) -> g(
  g.rect(
    g.position "CrimeType", "ArrestRate"
)
g.from inspect "data", getFrame "frame_rdd_119"
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

  g.from inspect "data", getFrame "frame_rdd_119"
)
*/
