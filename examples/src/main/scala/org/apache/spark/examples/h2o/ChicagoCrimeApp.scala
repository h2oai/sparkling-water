/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.examples.h2o

import hex.Distribution.Family
import hex.deeplearning.DeepLearningModel
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import hex.tree.gbm.GBMModel
import hex.{Model, ModelMetricsBinomial}
import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTimeConstants._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, MutableDateTime}
import water.MRTask
import water.app.{ModelMetricsSupport, SparkContextSupport, SparklingWaterApp}
import water.fvec.{Chunk, NewChunk, Vec}
import water.parser.BufferedString

/**
 * Chicago Crimes Application predicting probability of arrest in Chicago.
 */
class ChicagoCrimeApp( weatherFile: String,
                       censusFile: String,
                       crimesFile: String,
                       val datePattern: String = "MM/dd/yyyy hh:mm:ss a",
                       val dateTimeZone: String = "Etc/UTC")
                     ( @transient override val sc: SparkContext,
                       @transient override val sqlContext: SQLContext,
                       @transient override val h2oContext: H2OContext) extends SparklingWaterApp
                      with ModelMetricsSupport {

  def train(weatherTable: DataFrame, censusTable: DataFrame, crimesTable: DataFrame): (GBMModel, DeepLearningModel) = {
    // Prepare environment
    @transient implicit val sqlc = sqlContext
    @transient implicit val h2oc = h2oContext
    import h2oContext.implicits._

    // Register tables in SQL Context
    weatherTable.registerTempTable("chicagoWeather")
    censusTable.registerTempTable("chicagoCensus")
    crimesTable.registerTempTable("chicagoCrime")

    //
    // Join crime data with weather and census tables
    //
    val crimeWeather = sqlContext.sql(
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

    //crimeWeather.printSchema()
    val crimeWeatherDF:H2OFrame = crimeWeather
    // Transform all string columns into categorical
    DemoUtils.allStringVecToCategorical(crimeWeatherDF)

    //
    // Split final data table
    //
    import org.apache.spark.examples.h2o.DemoUtils._
    val keys = Array[String]("train.hex", "test.hex")
    val ratios = Array[Double](0.8, 0.2)
    val frs = splitFrame(crimeWeatherDF, keys, ratios)
    val (train, test) = (frs(0), frs(1))

    //
    // Build GBM model and collect model metrics
    //
    val gbmModel = GBMModel(train, test, 'Arrest)
    val (trainMetricsGBM, testMetricsGBM) = binomialMetrics(gbmModel, train, test)

    //
    // Build Deep Learning model and collect model metrics
    //
    val dlModel = DLModel(train, test, 'Arrest)
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

    (gbmModel, dlModel)
  }

  def GBMModel(train: H2OFrame, test: H2OFrame, response: String,
               ntrees:Int = 10, depth:Int = 6, family: Family = Family.bernoulli)
              (implicit h2oContext: H2OContext) : GBMModel = {
    import h2oContext.implicits._
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._valid = test
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
               activation: Activation = Activation.RectifierWithDropout, hidden:Array[Int] = Array(200,200))
             (implicit h2oContext: H2OContext) : DeepLearningModel = {
    import h2oContext.implicits._
    import hex.deeplearning.DeepLearning

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

  def binomialMetrics[M <: Model[M,P,O], P <: hex.Model.Parameters, O <: hex.Model.Output]
                (model: Model[M,P,O], train: H2OFrame, test: H2OFrame):(ModelMetricsBinomial, ModelMetricsBinomial) = {
    (modelMetrics(model,train), modelMetrics(model, test))
  }


  /** Load all data from given 3 sources and returns Spark's DataFrame for each of them.
    *
    * @return tuple of weather, census and crime data
    */
  def loadAll(): (DataFrame, DataFrame, DataFrame) = {
    implicit val sqlc = sqlContext
    implicit val h2oc = h2oContext
    import h2oContext._
    // Weather data
    val weatherTable = asDataFrame(createWeatherTable(weatherFile))
    weatherTable.registerTempTable("chicagoWeather")
    // Census data
    val censusTable = asDataFrame(createCensusTable(censusFile))
    censusTable.registerTempTable("chicagoCensus")
    // Crime data
    val crimeTable  = asDataFrame(createCrimeTable(crimesFile))
    crimeTable.registerTempTable("chicagoCrime")

    (weatherTable, censusTable, crimeTable)
  }

  private def loadData(datafile: String): H2OFrame = new H2OFrame(new java.net.URI(datafile))

  def createWeatherTable(datafile: String): H2OFrame = {
    val table = loadData(datafile)
    // Remove first column since we do not need it
    table.remove(0).remove()
    table.update()
    table
  }

  def createCensusTable(datafile: String): H2OFrame = {
    val table = loadData(datafile)
    // Rename columns: replace ' ' by '_'
    val colNames = table.names().map( n => n.trim.replace(' ', '_').replace('+','_'))
    table._names = colNames
    table.update()
    table
  }

  def createCrimeTable(datafile: String): H2OFrame = {
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
    table.update()
    table
  }

  /**
   * For given crime and model returns probability of crime.
   *
   * @param crime
   * @param model
   * @param censusTable
   * @param sqlContext
   * @param h2oContext
   * @return
   */
  def scoreEvent(crime: Crime, model: Model[_,_,_], censusTable: DataFrame)
                (implicit sqlContext: SQLContext, h2oContext: H2OContext): Float = {
    import h2oContext.implicits._
    import sqlContext.implicits._
    // Create a single row table
    val srdd: DataFrame = sqlContext.sparkContext.parallelize(Seq(crime)).toDF
    // Join table with census data
    val row: H2OFrame = censusTable.join(srdd).where('Community_Area === 'Community_Area_Number) //.printSchema
    // Transform all string columns into categorical
    DemoUtils.allStringVecToCategorical(row)

    val predictTable = model.score(row)
    val probOfArrest = predictTable.vec("true").at(0)

    probOfArrest.toFloat
  }

}

object ChicagoCrimeApp extends SparkContextSupport {

  def main(args: Array[String]) {
    // Prepare environment
    val sc = new SparkContext(configure("ChicagoCrimeTest"))
    // SQL support
    val sqlContext = new SQLContext(sc)
    // Start H2O services
    val h2oContext = H2OContext.getOrCreate(sc)

    val app = new ChicagoCrimeApp(
                weatherFile = "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoAllWeather.csv",
                censusFile =  "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoCensus.csv",
                crimesFile =  "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoCrimes.csv")(sc, sqlContext, h2oContext)
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
          |
        """.stripMargin)
    }

    // Shutdown full stack
    app.shutdown()
  }

  def SEASONS = Array[String]("Spring", "Summer", "Autumn", "Winter")

  def getSeason(month: Int) =
    if (month >= MARCH && month <= MAY) 0 // Spring
    else if (month >= JUNE && month <= AUGUST) 1 // Summer
    else if (month >= SEPTEMBER && month <= OCTOBER) 2 // Autumn
    else 3 // Winter
}
// scalastyle:off rddtype
case class Crime(Year: Short, Month: Byte, Day: Byte, WeekNum: Byte, HourOfDay: Byte,
                 Weekend:Byte, Season: String, WeekDay: Byte,
                 IUCR: Short,
                 Primary_Type: String,
                 Location_Description: String,
                 Domestic: String,
                 Beat: Short,
                 District: Byte,
                 Ward: Byte,
                 Community_Area: Byte,
                 FBI_Code: Byte,
                 minTemp: Option[Byte],
                 maxTemp: Option[Byte],
                 meanTemp: Option[Byte])

object Crime {
  def apply(date:String,
            iucr: Short,
            primaryType: String,
            locationDescr: String,
            domestic: Boolean,
            beat: Short,
            district: Byte,
            ward: Byte,
            communityArea: Byte,
            fbiCode: Byte,
            minTemp: Option[Byte] = None,
            maxTemp: Option[Byte] = None,
            meanTemp: Option[Byte] = None,
            datePattern: String = "MM/dd/yyyy hh:mm:ss a",
            dateTimeZone: String = "Etc/UTC"):Crime = {
    val dtFmt = DateTimeFormat.forPattern(datePattern).withZone(DateTimeZone.forID(dateTimeZone))
    val mDateTime = new MutableDateTime()
    dtFmt.parseInto(mDateTime, date, 0)
    val month = mDateTime.getMonthOfYear.toByte
    val dayOfWeek = mDateTime.getDayOfWeek

    Crime(mDateTime.getYear.toShort,
      month,
      mDateTime.getDayOfMonth.toByte,
      mDateTime.getWeekOfWeekyear.toByte,
      mDateTime.getHourOfDay.toByte,
      if (dayOfWeek == SUNDAY || dayOfWeek == SATURDAY) 1 else 0,
      ChicagoCrimeApp.SEASONS(ChicagoCrimeApp.getSeason(month)),
      mDateTime.getDayOfWeek.toByte,
      iucr, primaryType, locationDescr,
      if (domestic) "true" else "false" ,
      beat, district, ward, communityArea, fbiCode,
      minTemp, maxTemp, meanTemp)
  }
}
// scalastyle:on rddtype

/**
 * Adhoc date column refinement.
 *
 * It takes column in the specified format 'MM/dd/yyyy hh:mm:ss a' and refines
 * it into 8 columns: "Day", "Month", "Year", "WeekNum", "WeekDay", "Weekend", "Season", "HourOfDay"
 */
class RefineDateColumn(val datePattern: String,
                       val dateTimeZone: String) extends MRTask[RefineDateColumn] {
  // Entry point
  def doIt(col: Vec): H2OFrame = {
    val inputCol = if (col.isCategorical) col.toStringVec else col
    val result = new H2OFrame(
      doAll(Array[Byte](Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM), inputCol).outputFrame(
        Array[String]("Day", "Month", "Year", "WeekNum", "WeekDay", "Weekend", "Season", "HourOfDay"),
        Array[Array[String]](null, null, null, null, null, null, ChicagoCrimeApp.SEASONS, null)))
    if (col.isCategorical) inputCol.remove()
    result
  }

  override def map(cs: Array[Chunk], ncs: Array[NewChunk]): Unit = {
    // Initialize DataTime convertor (cannot be done in setupLocal since it is not H2O serializable :-/
    val dtFmt = DateTimeFormat.forPattern(datePattern).withZone(DateTimeZone.forID(dateTimeZone))
    // Get input and output chunks
    val dateChunk = cs(0)
    val (dayNC, monthNC, yearNC, weekNC, weekdayNC, weekendNC, seasonNC, hourNC)
    = (ncs(0), ncs(1), ncs(2), ncs(3), ncs(4), ncs(5), ncs(6), ncs(7))
    val valStr = new BufferedString()
    val mDateTime = new MutableDateTime()
    for(row <- 0 until dateChunk.len()) {
      if (dateChunk.isNA(row)) {
        addNAs(ncs)
      } else {
        // Extract data
        val ds = dateChunk.atStr(valStr, row).toString
        if (dtFmt.parseInto(mDateTime, ds, 0) > 0) {
          val month = mDateTime.getMonthOfYear
          dayNC.addNum(mDateTime.getDayOfMonth, 0)
          monthNC.addNum(month, 0)
          yearNC.addNum(mDateTime.getYear, 0)
          weekNC.addNum(mDateTime.getWeekOfWeekyear)
          val dayOfWeek = mDateTime.getDayOfWeek
          weekdayNC.addNum(dayOfWeek)
          weekendNC.addNum(if (dayOfWeek == SUNDAY || dayOfWeek == SATURDAY) 1 else 0, 0)
          seasonNC.addNum(ChicagoCrimeApp.getSeason(month), 0)
          hourNC.addNum(mDateTime.getHourOfDay)
        } else {
          addNAs(ncs)
        }
      }
    }
  }

  private def addNAs(ncs: Array[NewChunk]): Unit = ncs.foreach(nc => nc.addNA())
}
