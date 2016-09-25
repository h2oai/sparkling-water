/**
 * Launch following commands:
 *   export MASTER='local-cluster[3,2,4512]'
 *   bin/sparkling-shell -i examples/scripts/chicagoCrime.script.scala --conf "spark.executor.memory=4G"
 *
 * When running using spark shell or using scala rest API:
 *    SQLContext is available as sqlContext
 *     - if you want to use sqlContext implicitly, you have to redefine it like: implicit val sqlContext = sqlContext,
 *      but better is to use it like this: implicit val sqlContext = SQLContext.getOrCreate(sc)
 *    SparkContext is available as sc
 *
 * Needs access to H2O internal HDFS storage or change paths below.
 *
 */
import org.apache.spark.examples.h2o.{Crime, ChicagoCrimeApp}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SQLContext

// Create SQL support
implicit val sqlContext = spark.sqlContext
// Start H2O services
implicit val h2oContext = H2OContext.getOrCreate(sc)

val app = new ChicagoCrimeApp(
  weatherFile = "hdfs://mr-0xd6.0xdata.loc/datasets/chicagoAllWeather.csv",
  censusFile =  "hdfs://mr-0xd6.0xdata.loc/datasets/chicagoCensus.csv",
  crimesFile =  "hdfs://mr-0xd6.0xdata.loc/datasets/chicagoCrimes.csv")(sc, sqlContext, h2oContext)

// Load data
val (weatherTable,censusTable,crimesTable) = app.loadAll()

// Train model
val (gbmModel, dlModel) = app.train(weatherTable, censusTable, crimesTable)

val crimeExamples = Seq(
  Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET",false, 422, 4, 7, 46, 18),
  Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE",false, 923, 9, 14, 63, 11))

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

/*
// Group by type of crimes
val q1 = "SELECT Primary_Type, count(*) FROM chicagoCrime GROUP BY Primary_Type"
val allCrimes = sqlContext.sql(q1).collect // 35 items
//Filter only successful arrests
val q2 = "SELECT Primary_Type, count(*) FROM chicagoCrime WHERE Arrest = 'true' GROUP BY Primary_Type"
val crimesWithArrest = sqlContext.sql(q2).collect
val crimeTypeToArrest = collection.mutable.Map[String, Long]()
allCrimes.foreach( c => if (!c.isNullAt(0)) crimeTypeToArrest += ( c.getString(0) -> c.getLong(1) ) )
import org.apache.spark.sql._
val numOfAllCrimes = crimesTable.count
val numOfAllArrests = sqlContext.sql("SELECT * FROM chicagoCrime WHERE Arrest = 'true'").count
val crimeTypeArrestRate = crimesWithArrest.map(c =>
  if (!c.isNullAt(0)) {
    val crimeType = c.getString(0)
    val count:Long = crimeTypeToArrest.get(crimeType).getOrElse(0)
    Row(crimeType, c.getLong(1).toDouble/count, c.getLong(1), count, c.getLong(1)/numOfAllArrests.toDouble, c.getLong(1)/count.toDouble, count/numOfAllCrimes.toDouble) } ).map(_.asInstanceOf[Row])
import org.apache.spark.sql.types._
val schema = StructType(Seq(
  StructField("CrimeType", StringType, false),
  StructField("ArrestRate", DoubleType, false),
  StructField("NumOfArrests", LongType, false),
  StructField("NumOfCrimes", LongType, false),
  StructField("ArrestsToAllArrests", DoubleType, false),
  StructField("ArrestsToAllCrimes", DoubleType, false),
  StructField("CrimessToCrimes", DoubleType, false)))
val rowRdd = sc.parallelize(crimeTypeArrestRate).sortBy(x => -x.getDouble(1))
val rateSRdd = sqlContext.applySchema(rowRdd, schema)

import water.fvec.H2OFrame; import h2oContext._
// Transfer it into H2O
val rateFrame:H2OFrame = rateSRdd
*/
/*
In flow type this:
plot (g) -> g(
  g.rect(
    g.position "CrimeType", "ArrestRate"
)
g.from inspect "data", getFrame "frame_rdd_132"
)

or

plot (g) -> g(
  g.rect(
    g.position "CrimeType", "ArrestRate"
    g.fillColor g.value 'blue'
    g.fillOpacity g.value 0.75

  )
  g.rect(
    g.position "CrimeType", "CrimessToCrimes"
    g.fillColor g.value 'red'
    g.fillOpacity g.value 0.65

  )

  g.from inspect "data", getFrame "frame_rdd_129"
)
*/

