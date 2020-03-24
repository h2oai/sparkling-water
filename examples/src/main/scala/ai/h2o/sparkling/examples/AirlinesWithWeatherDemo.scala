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
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AirlinesWithWeatherDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Join of Airlines with Weather Data")
      .getOrCreate()
    import spark.implicits._

    val weatherDataPath = "./examples/smalldata/chicago/Chicago_Ohare_International_Airport.csv"
    val weatherDataFile = s"file://${new File(weatherDataPath).getAbsolutePath}"
    val weatherTable = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(weatherDataFile)
      .withColumn("Date", to_date(regexp_replace('Date, "(\\d+)/(\\d+)/(\\d+)", "$3-$2-$1")))
      .withColumn("Year", year('Date))
      .withColumn("Month", month('Date))
      .withColumn("DayofMonth", dayofmonth('Date))

    val airlinesDataPath = "./examples/smalldata/airlines/allyears2k_headers.csv"
    val airlinesDataFile = s"file://${new File(airlinesDataPath).getAbsolutePath}"
    val airlinesTable = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA")
      .csv(airlinesDataFile)

    val flightsToORD = airlinesTable.filter('Dest === "ORD")

    println(s"\nFlights to ORD: ${flightsToORD.count}\n")

    val joined = flightsToORD.join(weatherTable, Seq("Year", "Month", "DayofMonth"))

    H2OContext.getOrCreate()
    val dl = new H2ODeepLearning()
      .setLabelCol("ArrDelay")
      .setSplitRatio(0.8)
      .setEpochs(5)
      .setHidden(Array(100, 100))
      .setActivation("RectifierWithDropout")
    val deepLearningModel = dl.fit(joined)

    val predictionsFromDL = deepLearningModel.transform(joined).select("prediction").collect()
    println(predictionsFromDL.mkString("\n===> Model predictions from DL: ", ", ", ", ...\n"))

    val gbm = new H2OGBM()
      .setLabelCol("ArrDelay")
      .setSplitRatio(0.8)
      .setNtrees(100)
    val gbmModel = gbm.fit(joined)

    val predictionsFromGBM = gbmModel.transform(joined).select("prediction").collect()
    println(predictionsFromGBM.mkString("\n===> Model predictions from GBM: ", ", ", ", ...\n"))
  }
}
