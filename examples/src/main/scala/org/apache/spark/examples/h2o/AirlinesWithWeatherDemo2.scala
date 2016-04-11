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

import hex.FrameSplitter
import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import DeepLearningParameters.Activation
import hex.tree.gbm.GBM
import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.spark.examples.h2o.DemoUtils.residualPlotRCode
import org.apache.spark.h2o.{Frame, H2OContext, H2OFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkFiles, SparkConf, SparkContext}
import water.Key
import water.app.SparkContextSupport

/** Demo for meetup presented at 12/17/2014 */
object AirlinesWithWeatherDemo2 extends SparkContextSupport {

  def main(args: Array[String]): Unit = {
    // Configure this application
    val conf: SparkConf = configure("Sparkling Water Meetup: Use Airlines and Weather Data for delay prediction")
    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._
    // Setup environment
    addFiles(sc,
      absPath("examples/smalldata/Chicago_Ohare_International_Airport.csv"),
      absPath("examples/smalldata/year2005.csv.gz"))

    //val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
    val wrawdata = sc.textFile(enforceLocalSparkFile("Chicago_Ohare_International_Airport.csv"),3).cache()
    val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())

    //
    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val airlinesData = new H2OFrame(new File(SparkFiles.get("year2005.csv.gz")))

    val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
    // Select flights only to ORD
    val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))

    flightsToORD.count
    println(s"\nFlights to ORD: ${flightsToORD.count}\n")

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._ // import implicit conversions
    flightsToORD.toDF.registerTempTable("FlightsToORD")
    weatherTable.toDF.registerTempTable("WeatherORD")

    //
    // -- Join both tables and select interesting columns
    //
    val joinedTable = sqlContext.sql(
      """SELECT
        |f.Year,f.Month,f.DayofMonth,
        |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
        |f.UniqueCarrier,f.FlightNum,f.TailNum,
        |f.Origin,f.Distance,
        |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
        |f.ArrDelay
        |FROM FlightsToORD f
        |JOIN WeatherORD w
        |ON f.Year=w.Year AND f.Month=w.Month AND f.DayofMonth=w.Day""".stripMargin)
    println(s"\nResult of query: ${joinedTable.count}\n")

    //
    // Split data into 3 tables - train/validation/test
    //
    // Instead of using RDD API we will directly split H2O Frame
    val joinedH2OFrame:H2OFrame = joinedTable // Invoke implicit transformation
    // Transform date related columns to enums
    for( i <- 0 to 2) joinedH2OFrame.replace(i, joinedH2OFrame.vec(i).toCategoricalVec)

    //
    // Use low-level task to split the frame
    val sf = new FrameSplitter(joinedH2OFrame, Array(.7, .2), Array("train", "valid","test").map(Key.make[Frame](_)), null)
    water.H2O.submitTask(sf)
    val splits = sf.getResult

    val trainTable = splits(0)
    val validTable = splits(1)
    val testTable  = splits(2)

    //
    // -- Run DeepLearning
    //
    val dlParams = new DeepLearningParameters()
    dlParams._train = trainTable
    dlParams._response_column = 'ArrDelay
    dlParams._valid = validTable
    dlParams._epochs = 5
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](100, 100)
    dlParams._reproducible = true
    dlParams._force_load_balance = false

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    val dlPredictTable = dlModel.score(testTable)('predict)
    val predictionsFromDlModel = asDataFrame(dlPredictTable).collect
                                .map(row => if (row.isNullAt(0)) Double.NaN else row(0))

    println(predictionsFromDlModel.length)
    println(predictionsFromDlModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    printf( residualPlotRCode(dlPredictTable, 'predict, testTable, 'ArrDelay) )

    // GBM Model
    val gbmParams = new GBMParameters()
    gbmParams._train = trainTable
    gbmParams._response_column = 'ArrDelay
    gbmParams._valid = validTable
    gbmParams._ntrees = 100
    //gbmParams._learn_rate = 0.01f
    val gbm = new GBM(gbmParams)
    val gbmModel = gbm.trainModel.get

    // Print R code for residual plot
    val gbmPredictTable = gbmModel.score(testTable)('predict)
    printf( residualPlotRCode(gbmPredictTable, 'predict, testTable, 'ArrDelay) )

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
