/**
 * Launch following commands:
 *   export MASTER='local-cluster[3,2,1024]'
 *   bin/sparkling-shell -i examples/scripts/chicagoCrimeSmall.script.scala
 *
  * When running using spark shell or using scala rest API:
  *    SQLContext is available as sqlContext
  *     - if you want to use sqlContext implicitly, you have to redefine it like: implicit val sqlContext = sqlContext,
  *      but better is to use it like this: implicit val sqlContext = SQLContext.getOrCreate(sc)
  *    SparkContext is available as sc
  */
// 1. Create an environment
import org.apache.spark.SparkFiles
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.examples.h2o.{Crime, ChicagoCrimeApp}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

// 2. Register local files
addFiles(sc,
  "examples/smalldata/chicagoAllWeather.csv",
  "examples/smalldata/chicagoCensus.csv",
  "examples/smalldata/chicagoCrimes10k.csv"
)

// 3. Create SQL support
implicit val sqlContext = SQLContext.getOrCreate(sc)
// 4. Start H2O services
implicit val h2oContext = H2OContext.getOrCreate(sc)

// 5. Create App
val app = new ChicagoCrimeApp(
  weatherFile = SparkFiles.get("chicagoAllWeather.csv"),
  censusFile = SparkFiles.get("chicagoCensus.csv"),
  crimesFile = SparkFiles.get("chicagoCrimes10k.csv"))(sc, sqlContext, h2oContext)

// 6. Load data
val (weatherTable,censusTable,crimesTable) = app.loadAll()

// 7. Train model
val (gbmModel, dlModel) = app.train(weatherTable, censusTable, crimesTable)

// 8. Create list of crimes to predict
val crimeExamples = Seq(
  Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET",false, 422, 4, 7, 46, 18),
  Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE",false, 923, 9, 14, 63, 11))

// 8. Score each crime and predict probability of arrest
for (crime <- crimeExamples) {
  val arrestProbGBM = 100*app.scoreEvent(crime,
    gbmModel,
    censusTable)(sqlContext, h2oContext)
  val arrestProbDL = 100*app.scoreEvent(crime,
    dlModel,
    censusTable)(sqlContext, h2oContext)
  println(
    s"""
       |Crime: $crime
       |  Probability of arrest best on DeepLearning: ${arrestProbDL} %
       |  Probability of arrest best on GBM: ${arrestProbGBM} %
        """.stripMargin)
}
