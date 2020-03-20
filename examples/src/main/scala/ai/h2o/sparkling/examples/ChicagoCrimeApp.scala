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

package ai.h2o.sparkling.examples

import hex.deeplearning.DeepLearningModel
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import hex.genmodel.utils.DistributionFamily
import hex.tree.gbm.GBMModel
import hex.{Model, ModelMetricsBinomial}
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import water.fvec.Vec
import water.parser.ParseSetup
import water.support.{H2OFrameSupport, ModelMetricsSupport}

/**
 * Chicago Crimes Application predicting probability of arrest in Chicago.
 */
class ChicagoCrimeApp(weatherFile: String,
                      censusFile: String,
                      crimesFile: String)
                     (@transient val hc: H2OContext) extends ModelMetricsSupport with H2OFrameSupport {

  @transient private val spark = hc.sparkSession

  /** Load all data from given 3 sources and returns Spark's DataFrame for each of them.
   *
   * @return tuple of weather, census and crime data
   */
  def loadAll(): (DataFrame, DataFrame, DataFrame) = {
    // Weather data
    val weatherTable = createWeatherTable(weatherFile)
    weatherTable.createOrReplaceTempView("chicagoWeather")
    // Census data
    val censusTable = createCensusTable(censusFile)
    censusTable.createOrReplaceTempView("chicagoCensus")
    // Crime data
    val crimeTable = createCrimeTable(crimesFile)
    crimeTable.createOrReplaceTempView("chicagoCrime")

    (weatherTable, censusTable, crimeTable)
  }

  def train(weatherTable: DataFrame, censusTable: DataFrame, crimesTable: DataFrame): (GBMModel, DeepLearningModel) = {
    // Register tables in SQL Context
    weatherTable.createOrReplaceTempView("chicagoWeather")
    censusTable.createOrReplaceTempView("chicagoCensus")
    crimesTable.createOrReplaceTempView("chicagoCrime")

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

    val crimeWeatherDF: H2OFrame = hc.asH2OFrame(crimeWeather)
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
    // Build GBM model and collect model metrics
    //
    val gbmModel = GBMModel(train, test, "Arrest")
    val (trainMetricsGBM, testMetricsGBM) = binomialMetrics(gbmModel, train, test)

    //
    // Build Deep Learning model and collect model metrics
    //
    val dlModel = DLModel(train, test, "Arrest")
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

  def score(crimes: Seq[Crime], gbmModel: GBMModel, dlModel: DeepLearningModel, censusTable: DataFrame): Unit = {
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
  }

  private def GBMModel(train: H2OFrame, test: H2OFrame, response: String,
                       ntrees: Int = 10, depth: Int = 6, family: DistributionFamily = DistributionFamily.bernoulli): GBMModel = {
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

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

  private def DLModel(train: H2OFrame, test: H2OFrame, response: String,
                      epochs: Int = 10, l1: Double = 0.0001, l2: Double = 0.0001,
                      activation: Activation = Activation.RectifierWithDropout, hidden: Array[Int] = Array(200, 200)): DeepLearningModel = {
    import hex.deeplearning.DeepLearning

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

  private def binomialMetrics[M <: Model[M, P, O], P <: hex.Model.Parameters, O <: hex.Model.Output]
  (model: Model[M, P, O], train: H2OFrame, test: H2OFrame): (ModelMetricsBinomial, ModelMetricsBinomial) = {
    (modelMetrics(model, train), modelMetrics(model, test))
  }

  private def loadData(datafile: String, modifyParserSetup: ParseSetup => ParseSetup = identity[ParseSetup]): H2OFrame = {
    val uri = java.net.URI.create(datafile)
    val parseSetup = modifyParserSetup(water.fvec.H2OFrame.parserSetup(uri))
    new H2OFrame(parseSetup, new java.net.URI(datafile))
  }

  private def createWeatherTable(datafile: String): DataFrame = {
    val table = loadData(datafile)
    val fr = withLockAndUpdate(table) {
      // Remove first column since we do not need it
      _.remove(0).remove()
    }
    hc.asDataFrame(fr)
  }

  private def createCensusTable(datafile: String): DataFrame = {
    val table = loadData(datafile)
    val fr = withLockAndUpdate(table) { fr =>
      val colNames = fr.names().map(n => n.trim.replace(' ', '_').replace('+', '_'))
      fr._names = colNames
    }
    hc.asDataFrame(fr)
  }

  private def addAdditionalDateColumns(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    df
      .withColumn("Date", from_unixtime(unix_timestamp('Date, "MM/dd/yyyy hh:mm:ss a")))
      .withColumn("Year", year('Date))
      .withColumn("Month", month('Date))
      .withColumn("Day", dayofmonth('Date))
      .withColumn("WeekNum", weekofyear('Date))
      .withColumn("HourOfDay", hour('Date))
      .withColumn("Season", ChicagoCrimeApp.seasonUdf('Month))
      .withColumn("WeekDay", date_format('Date, "u"))
      .withColumn("Weekend", ChicagoCrimeApp.weekendUdf('WeekDay))
      .drop('Date)
  }

  private def createCrimeTable(datafile: String): DataFrame = {
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

  private def scoreEvent(crime: Crime, model: Model[_, _, _], censusTable: DataFrame): Float = {
    import spark.implicits._
    // Create Spark DataFrame from a single row
    val df = addAdditionalDateColumns(spark.sparkContext.parallelize(Seq(crime)).toDF)
      .withColumn("Domestic", 'Domestic.cast(StringType))
    // Join table with census data
    val row: H2OFrame = hc.asH2OFrame(censusTable.join(df).where('Community_Area === 'Community_Area_Number))
    // Transform all string columns into categorical
    allStringVecToCategorical(row)

    val predictTable = model.score(row)
    val probOfArrest = predictTable.vec("true").at(0)

    probOfArrest.toFloat
  }
}

object ChicagoCrimeApp {

  private val seasonUdf = udf(getSeason _)
  private val weekendUdf = udf(isWeekend _)

  private def SEASONS: Array[String] = Array[String]("Spring", "Summer", "Autumn", "Winter")

  private def getSeason(month: Int): String = {
    val seasonNum =
      if (month >= 3 && month <= 5) 0 // Spring
      else if (month >= 6 && month <= 8) 1 // Summer
      else if (month >= 9 && month <= 10) 2 // Autumn
      else 3 // Winter
    SEASONS(seasonNum)
  }

  private def isWeekend(dayOfWeek: Int): Int = if (dayOfWeek == 7 || dayOfWeek == 6) 1 else 0
}

case class Crime(date: String,
                 IUCR: Short,
                 Primary_Type: String,
                 Location_Description: String,
                 Domestic: Boolean,
                 Beat: Short,
                 District: Byte,
                 Ward: Byte,
                 Community_Area: Byte,
                 FBI_Code: Byte,
                 minTemp: Option[Byte] = None,
                 maxTemp: Option[Byte] = None,
                 meanTemp: Option[Byte] = None)
