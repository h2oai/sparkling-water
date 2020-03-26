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

import ai.h2o.sparkling.ml.algos.H2OGBM
import ai.h2o.sparkling.ml.models.H2OMOJOModel
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object CityBikeSharingDemo {

  val daysToTimestampUdf: UserDefinedFunction = udf(daysToTimestamp _)
  val numberOfDaysSinceEpochUdf: UserDefinedFunction = udf(numberOfDaysSinceEpoch _)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("City Bike Sharing Demo")
      .getOrCreate()

    val bikesPerDayTable = loadBikesData(spark)
    val bikesPerDayModel = buildModel(bikesPerDayTable)
    printPredictions(bikesPerDayModel, bikesPerDayTable)

    val bikesWeatherTable = loadBikesWeatherData(spark, bikesPerDayTable)
    val bikesWeatherModel = buildModel(bikesWeatherTable)
    printPredictions(bikesWeatherModel, bikesWeatherTable)
  }

  def loadBikesData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val bikesDataPath = "./examples/smalldata/citybike-nyc/citybike_2013.csv"
    val bikesDataFile = s"file://${new File(bikesDataPath).getAbsolutePath}"
    val bikesTable = spark.read.option("header", "true").option("inferSchema", "true").csv(bikesDataFile)

    removeSpacesFromColumnNames(bikesTable)
      .withColumn("Days", numberOfDaysSinceEpochUdf(unix_timestamp('starttime)))
      .groupBy("Days", "start_station_id").count()
      .withColumnRenamed("count", "bikes")
      .withColumn("date", from_unixtime(daysToTimestampUdf('Days)))
      .withColumn("Month", month('date))
      .withColumn("DayOfweek", date_format('date, "u"))
      .drop('date)
  }

  def removeSpacesFromColumnNames(input: DataFrame): DataFrame = {
    val renamedColumns = input.columns.map { col =>
      val name = col.trim.replaceAll("\\s", "_")
      input(col).as(name)
    }
    input.select(renamedColumns: _*)
  }

  def loadBikesWeatherData(spark: SparkSession, bikesPerDay: DataFrame): DataFrame = {
    import spark.implicits._
    val weatherDataPath = "./examples/smalldata/citybike-nyc/New_York_City_Hourly_Weather_2013.csv"
    val weatherDataFile = s"file://${new File(weatherDataPath).getAbsolutePath}"
    val weatherTable = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(weatherDataFile)
      .select("Hour Local", "Year Local", "Month Local", "Day Local", "Dew Point (C)", "Humidity Fraction",
        "Precipitation One Hour (mm)", "Weather Code 1", "Temperature (C)")
      .withColumnRenamed("Hour Local", "HourLocal")
      .withColumnRenamed("Year Local", "YearLocal")
      .withColumnRenamed("Day Local", "DayLocal")
      .withColumnRenamed("Month Local", "MonthLocal")
      .withColumnRenamed("Dew Point (C)", "DewPoint")
      .withColumnRenamed("Humidity Fraction", "HumidityFraction")
      .withColumnRenamed("Precipitation One Hour (mm)", "Prcp1Hour")
      .withColumnRenamed("Weather Code 1", "WeatherCode1")
      .withColumnRenamed("Temperature (C)", "Temperature")
      .withColumn("HourLocal", format_string("%02d", 'HourLocal.cast(IntegerType)))
      .withColumn("DayLocal", format_string("%02d", 'DayLocal.cast(IntegerType)))
      .withColumn("MonthLocal", format_string("%02d", 'MonthLocal.cast(IntegerType)))
      .withColumn("YearLocal", format_string("%04d", 'YearLocal.cast(IntegerType)))
      .filter('HourLocal === 12)
        .withColumn("Date", concat('YearLocal, 'MonthLocal, 'DayLocal, 'HourLocal))
        .withColumn("Days", numberOfDaysSinceEpochUdf(unix_timestamp('Date, "yyyyMMddHH")))

    bikesPerDay.join(weatherTable, Seq("Days"))
  }

  def buildModel(train: DataFrame): H2OMOJOModel = {
    H2OContext.getOrCreate()
    val gbm = new H2OGBM()
      .setNtrees(100)
      .setMaxDepth(6)
      .setLabelCol("bikes")
      .setSplitRatio(0.8)
    gbm.fit(train)
  }

  def printPredictions(model: H2OMOJOModel, input: DataFrame): Unit = {
    val predictions = model.transform(input).select("prediction").collect()
    println(predictions.mkString("\n===> Model predictions from GBM: ", ", ", ", ...\n"))
  }

  def daysToTimestamp(days: Int): String = (days * (60 * 60 * 24)).toString

  def numberOfDaysSinceEpoch(timestamp: Long): Long = timestamp / (60 * 60 * 24)
}


