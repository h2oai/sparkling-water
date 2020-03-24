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

import java.io.File

import ai.h2o.sparkling.ml.algos.{H2ODeepLearning, H2OGBM}
import ai.h2o.sparkling.ml.models.H2OMOJOModel
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

object ChicagoCrimeApp {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Chicago Crime App")
      .getOrCreate()

    val weatherDataPath = "./examples/smalldata//chicago/chicagoAllWeather.csv"
    val weatherDataFile = s"file://${new File(weatherDataPath).getAbsolutePath}"
    val weatherTable = createWeatherTable(spark, weatherDataFile)
    weatherTable.createOrReplaceTempView("chicagoWeather")

    val censusDataPath = "./examples/smalldata/chicago/chicagoCensus.csv"
    val censusDataFile = s"file://${new File(censusDataPath).getAbsolutePath}"
    val censusTable = createCensusTable(spark, censusDataFile)
    censusTable.createOrReplaceTempView("chicagoCensus")

    val crimesDataPath = "./examples/smalldata/chicago/chicagoCrimes10k.csv"
    val crimesDataFile = s"file://${new File(crimesDataPath).getAbsolutePath}"
    val crimesTable = createCrimeTable(spark, crimesDataFile)
    crimesTable.createOrReplaceTempView("chicagoCrime")

    // Join crimes and weather tables
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

    val gbmModel = trainGBM(crimeWeather)
    val dlModel = trainDeepLearning(crimeWeather)

    val crimes = Seq(
      Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET", Domestic = false, 422, 4, 7, 46, 18),
      Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE", Domestic = false, 923, 9, 14, 63, 11))
    score(spark, crimes, gbmModel, dlModel, censusTable)
  }

  def trainGBM(train: DataFrame): H2OMOJOModel = {
    H2OContext.getOrCreate()
    val gbm = new H2OGBM()
      .setSplitRatio(0.8)
      .setLabelCol("Arrest")
      .setColumnsToCategorical("Arrest")
      .setNtrees(10)
      .setMaxDepth(6)
      .setDistribution("bernoulli")
    gbm.fit(train)
  }

  def trainDeepLearning(train: DataFrame): H2OMOJOModel = {
    H2OContext.getOrCreate()
    val dl = new H2ODeepLearning()
      .setSplitRatio(0.8)
      .setLabelCol("Arrest")
      .setColumnsToCategorical("Arrest")
      .setEpochs(10)
      .setL1(0.0001)
      .setL2(0.0001)
      .setActivation("RectifierWithDropout")
      .setHidden(Array(200, 200))
    dl.fit(train)
  }

  def score(spark: SparkSession, crimes: Seq[Crime], gbmModel: H2OMOJOModel, dlModel: H2OMOJOModel, censusTable: DataFrame): Unit = {
    crimes.foreach { crime =>
      val arrestGBM = scoreEvent(spark, crime, gbmModel, censusTable)
      val arrestDL = scoreEvent(spark, crime, dlModel, censusTable)
      println(
        s"""
           |Crime: $crime
           |  Will be arrested based on DeepLearning: $arrestDL
           |  Will be arrested based on GBM: $arrestGBM
           |
        """.stripMargin)
    }
  }

  def createWeatherTable(spark: SparkSession, datafile: String): DataFrame = {
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(datafile)
    df.drop(df.columns(0))
  }

  def createCensusTable(spark: SparkSession, datafile: String): DataFrame = {
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(datafile)
    val renamedColumns = df.columns.map { col =>
      val name = col.trim.replace(' ', '_').replace('+', '_')
      df(col).as(name)
    }
    df.select(renamedColumns: _*)
  }

  def addAdditionalDateColumns(spark: SparkSession, df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
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

  def createCrimeTable(spark: SparkSession, datafile: String): DataFrame = {
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(datafile)
    val renamedColumns = df.columns.map { col =>
      val name = col.trim.replace(' ', '_').replace('+', '_')
      df(col).as(name)
    }
    addAdditionalDateColumns(spark, df.select(renamedColumns: _*))
  }

  def scoreEvent(spark: SparkSession, crime: Crime, model: H2OMOJOModel, censusTable: DataFrame): Boolean = {
    // Create Spark DataFrame from a single row
    import spark.implicits._
    val df = addAdditionalDateColumns(spark, spark.sparkContext.parallelize(Seq(crime)).toDF)
      .withColumn("Domestic", 'Domestic.cast(DoubleType))
    // Join table with census data
    val row = censusTable.join(df).where('Community_Area === 'Community_Area_Number)
    val predictTable = model.transform(row)
    predictTable.select("prediction").head().get(0).toString == "1"
  }

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

  case class Crime(date: String,
                   IUCR: Short,
                   Primary_Type: String,
                   Location_Description: String,
                   Domestic: Boolean,
                   Beat: Short,
                   District: Byte,
                   Ward: Byte,
                   Community_Area: Byte,
                   FBI_Code: Byte)

}
