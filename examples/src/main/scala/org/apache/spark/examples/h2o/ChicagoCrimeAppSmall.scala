package org.apache.spark.examples.h2o

import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkFiles}
import water.app.SparkContextSupport

/**
 * Chicago crime app on small data.
 */
object ChicagoCrimeAppSmall extends SparkContextSupport {

  def main(args: Array[String]) {
    // Prepare environment
    val sc = new SparkContext(configure("ChicagoCrimeTest"))
    // Add local files into the context
    addFiles(sc,
      absPath("examples/smalldata/chicagoAllWeather.csv"),
      absPath("examples/smalldata/chicagoCensus.csv"),
      absPath("examples/smalldata/chicagoCrimes10k.csv")
    )
    // SQL support
    val sqlContext = new SQLContext(sc)
    // Start H2O services
    val h2oContext = new H2OContext(sc).start()

    val app = new ChicagoCrimeApp(
      weatherFile = SparkFiles.get("chicagoAllWeather.csv"),
      censusFile = SparkFiles.get("chicagoCensus.csv"),
      crimesFile = SparkFiles.get("chicagoCrimes10k.csv"))(sc, sqlContext, h2oContext)

    // Load data
    val (weatherTable, censusTable, crimesTable) = app.loadAll()
    // Train model
    val (gbmModel, dlModel) = app.train(weatherTable, censusTable, crimesTable)

    val crimeExamples = Seq(
      Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET", false, 422, 4, 7, 46, 18),
      Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE", false, 923, 9, 14, 63, 11))

    for (crime <- crimeExamples) {
      val arrestProbGBM = 100 * app.scoreEvent(crime,
        gbmModel,
        censusTable)(sqlContext, h2oContext)
      val arrestProbDL = 100 * app.scoreEvent(crime,
        dlModel,
        censusTable)(sqlContext, h2oContext)
      println(
        s"""
           |Crime: $crime
            |  Probability of arrest best on DeepLearning: ${arrestProbDL} %
            |  Probability of arrest best on GBM: ${arrestProbGBM} %
                                                                                                                                    |
        """.stripMargin)
    }

    // Shutdown full stack
    app.shutdown(sc)
  }
}
