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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession
import water.api.TestUtils
import water.support.SparkContextSupport

/**
 * Chicago crime app on small data.
 */
object ChicagoCrimeAppSmall extends SparkContextSupport {

  def main(args: Array[String]) {
    // Prepare environment
    val sc = new SparkContext(configure("ChicagoCrimeTest"))

    // SQL support
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    // Start H2O services
    val h2oContext = H2OContext.getOrCreate(sc)

    val app = new ChicagoCrimeApp(
      weatherFile = TestUtils.locate("smalldata/chicago/chicagoAllWeather.csv"),
      censusFile = TestUtils.locate("smalldata/chicago/chicagoCensus.csv"),
      crimesFile = TestUtils.locate("smalldata/chicago/chicagoCrimes10k.csv.zip"))(sc, sqlContext, h2oContext)

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
    app.shutdown()
  }
}
