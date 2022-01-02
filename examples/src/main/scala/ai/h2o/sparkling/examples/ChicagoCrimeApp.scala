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

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.ml.algos.{H2ODeepLearning, H2OGBM}
import ai.h2o.sparkling.ml.models.H2OMOJOModel
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ChicagoCrimeApp {

  private val seasonUdf = udf(monthToSeason _)
  private val weekendUdf = udf((isWeekend _).andThen(boolToInt))
  private val dayOfWeekUdf = udf(dayOfWeek _)

  def main(args: Array[String]) {
    implicit val spark = SparkSession
      .builder()
      .appName("Chicago Crime App")
      .getOrCreate()
    import spark.implicits._

    val weatherTable = loadCsv("./examples/smalldata/chicago/chicagoAllWeather.csv").drop("date")
    val chicagoWeatherTableName = "chicagoWeather"
    weatherTable.createOrReplaceTempView(chicagoWeatherTableName)

    val censusTable = loadCsv("./examples/smalldata/chicago/chicagoCensus.csv")
    val chicagoCensusTableName = "chicagoCensus"
    censusTable.createOrReplaceTempView(chicagoCensusTableName)

    val crimesTable = addAdditionalDateColumns(loadCsv("./examples/smalldata/chicago/chicagoCrimes10k.csv"))
    val chicagoCrimeTableName = "chicagoCrime"
    crimesTable.createOrReplaceTempView(chicagoCrimeTableName)

    val crimeDataColumnsForTraining = Seq(
      $"cr.Year",
      $"cr.Month",
      $"cr.Day",
      $"WeekNum",
      $"HourOfDay",
      $"Weekend",
      $"Season",
      $"WeekDay",
      $"IUCR",
      $"Primary_Type",
      $"Location_Description",
      $"Community_Area",
      $"District",
      $"Arrest",
      $"Domestic",
      $"Beat",
      $"Ward",
      $"FBI_Code")

    val censusDataColumnsForTraining = Seq(
      $"PERCENT_AGED_UNDER_18_OR_OVER_64",
      $"PER_CAPITA_INCOME",
      $"HARDSHIP_INDEX",
      $"PERCENT_OF_HOUSING_CROWDED",
      $"PERCENT_HOUSEHOLDS_BELOW_POVERTY",
      $"PERCENT_AGED_16_UNEMPLOYED",
      $"PERCENT_AGED_25_WITHOUT_HIGH_SCHOOL_DIPLOMA")

    val weatherDataColumnsForTraining = Seq($"minTemp", $"maxTemp", $"meanTemp")

    val joinedDataForTraining = spark
      .table(chicagoCrimeTableName)
      .as("cr")
      .join(
        spark.table(chicagoWeatherTableName).as("we"),
        $"cr.Year" === $"we.year" and $"cr.Month" === $"we.month" and $"cr.Day" === $"we.day")
      .join(spark.table(chicagoCensusTableName).as("ce"), $"cr.Community_Area" === $"ce.Community_Area_Number")
      .select(crimeDataColumnsForTraining ++ censusDataColumnsForTraining ++ weatherDataColumnsForTraining: _*)

    H2OContext.getOrCreate()
    val gbmModel = trainGBM(joinedDataForTraining)
    val dlModel = trainDeepLearning(joinedDataForTraining)

    val crimesToScore = Seq(
      Crime(
        date = "02/08/2015 11:43:58 PM",
        IUCR = 1811,
        Primary_Type = "NARCOTICS",
        Location_Description = "STREET",
        Domestic = false,
        Beat = 422,
        District = 4,
        Ward = 7,
        Community_Area = 46,
        FBI_Code = 18),
      Crime(
        date = "02/08/2015 11:00:39 PM",
        IUCR = 1150,
        Primary_Type = "DECEPTIVE PRACTICE",
        Location_Description = "RESIDENCE",
        Domestic = false,
        Beat = 923,
        District = 9,
        Ward = 14,
        Community_Area = 63,
        FBI_Code = 11))
    score(crimesToScore, gbmModel, dlModel, censusTable)
  }

  private def trainGBM(train: DataFrame): H2OMOJOModel = {
    val gbm = new H2OGBM()
      .setSplitRatio(0.8)
      .setLabelCol("Arrest")
      .setColumnsToCategorical("Arrest")
      .setNtrees(10)
      .setMaxDepth(6)
      .setDistribution("bernoulli")
    gbm.fit(train)
  }

  private def trainDeepLearning(train: DataFrame): H2OMOJOModel = {
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

  private def score(crimes: Seq[Crime], gbmModel: H2OMOJOModel, dlModel: H2OMOJOModel, censusTable: DataFrame)(
      implicit spark: SparkSession): Unit = {
    crimes.foreach { crime =>
      val arrestGBM = scoreEvent(crime, gbmModel, censusTable)
      val arrestDL = scoreEvent(crime, dlModel, censusTable)
      println(s"""
           |Crime: $crime
           |  Will be arrested based on DeepLearning: $arrestDL
           |  Will be arrested based on GBM: $arrestGBM
           |
        """.stripMargin)
    }
  }

  private def scoreEvent(crime: Crime, model: H2OMOJOModel, censusTable: DataFrame)(
      implicit spark: SparkSession): Boolean = {
    import spark.implicits._
    val df = addAdditionalDateColumns(Seq(crime).toDF)
    // Join table with census data
    val row = censusTable.join(df).where('Community_Area === 'Community_Area_Number)
    val predictTable = model.transform(row)
    predictTable.collect().head.getAs[String]("prediction") == "1"
  }

  private def loadCsv(dataPath: String)(implicit spark: SparkSession): DataFrame = {
    val datafile = s"file://${new File(dataPath).getAbsolutePath}"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(datafile)
    val renamedColumns = df.columns.map { col =>
      val name = col.trim
        .replace(' ', '_')
        .replace('+', '_')
        .replace("__", "_")
      df(col).as(name)
    }
    df.select(renamedColumns: _*)
  }

  private def addAdditionalDateColumns(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    df.withColumn("DateTmp", from_unixtime(unix_timestamp('Date, "MM/dd/yyyy hh:mm:ss a")))
      .withColumn("Year", year('DateTmp))
      .withColumn("Month", month('DateTmp))
      .withColumn("Day", dayofmonth('DateTmp))
      .withColumn("WeekNum", weekofyear('DateTmp))
      .withColumn("HourOfDay", hour('DateTmp))
      .withColumn("Season", seasonUdf('Month))
      .withColumn("WeekDay", dayOfWeekUdf(date_format('DateTmp, "E")))
      .withColumn("Weekend", weekendUdf('WeekDay))
      .drop('DateTmp)
  }

  private def monthToSeason(month: Int): String = {
    if (month >= 3 && month <= 5) "Spring"
    else if (month >= 6 && month <= 8) "Summer"
    else if (month >= 9 && month <= 10) "Autumn"
    else "Winter"
  }

  private def isWeekend(dayOfWeek: Int): Boolean = dayOfWeek == 7 || dayOfWeek == 6

  private def boolToInt(bool: Boolean): Int = if (bool) 1 else 0

  private def dayOfWeek(day: String): Int = {
    day match {
      case "Mon" => 1
      case "Tue" => 2
      case "Wed" => 3
      case "Thu" => 4
      case "Fri" => 5
      case "Sat" => 6
      case "Sun" => 7
      case _ => throw new RuntimeException("Invalid day!")
    }
  }

  private case class Crime(
      date: String,
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
