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

import java.io.File

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import DeepLearningParameters.Activation
import org.apache.spark.h2o.{DoubleHolder, H2OContext, H2OFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkFiles, SparkConf, SparkContext}
import water.app.SparkContextSupport


object AirlinesWithWeatherDemo extends SparkContextSupport {

  def main(args: Array[String]): Unit = {
    // Configure this application
    val conf: SparkConf = configure("Sparkling Water: Join of Airlines with Weather Data")

    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._
    // Setup environment
    addFiles(sc,
      absPath("examples/smalldata/Chicago_Ohare_International_Airport.csv"),
      absPath("examples/smalldata/allyears2k_headers.csv.gz"))

    //val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
    val wrawdata = sc.textFile(enforceLocalSparkFile("Chicago_Ohare_International_Airport.csv"),3).cache()
    val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())

    //
    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val airlinesData = new H2OFrame(new File(SparkFiles.get("allyears2k_headers.csv.gz")))

    val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
    // Select flights only to ORD
    val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))

    flightsToORD.count
    println(s"\nFlights to ORD: ${flightsToORD.count}\n")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._ // import implicit conversions
    flightsToORD.toDF.registerTempTable("FlightsToORD")
    weatherTable.toDF.registerTempTable("WeatherORD")

    //
    // -- Join both tables and select interesting columns
    //
    val bigTable = sqlContext.sql(
      """SELECT
        |f.Year,f.Month,f.DayofMonth,
        |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
        |f.UniqueCarrier,f.FlightNum,f.TailNum,
        |f.Origin,f.Distance,
        |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
        |f.ArrDelay
        |FROM FlightsToORD f
        |JOIN WeatherORD w
        |ON f.Year=w.Year AND f.Month=w.Month AND f.DayofMonth=w.Day
        |WHERE f.ArrDelay IS NOT NULL""".stripMargin)

    val train: H2OFrame = bigTable .repartition(4) // This is trick to handle PUBDEV-928 - DeepLearning is failing on empty chunks

    //
    // -- Run DeepLearning
    //
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._response_column = 'ArrDelay
    dlParams._epochs = 5
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](100, 100)

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    val predictionH2OFrame = dlModel.score(bigTable)('predict)
    val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    println(
      s"""# R script for residual plot
        |library(h2o)
        |h = h2o.init()
        |
        |pred = h2o.getFrame(h, "${predictionH2OFrame._key}")
        |act = h2o.getFrame (h, "${bigTable._key}")
        |
        |predDelay = pred$$predict
        |actDelay = act$$ArrDelay
        |
        |nrow(actDelay) == nrow(predDelay)
        |
        |residuals = predDelay - actDelay
        |
        |compare = cbind (as.data.frame(actDelay$$ArrDelay), as.data.frame(residuals$$predict))
        |nrow(compare)
        |plot( compare[,1:2] )
        |
      """.stripMargin)

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
