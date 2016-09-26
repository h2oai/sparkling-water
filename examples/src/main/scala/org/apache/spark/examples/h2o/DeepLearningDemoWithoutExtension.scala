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
import org.apache.spark.h2o.{DoubleHolder, H2OContext, H2OFrame}
import org.apache.spark.{SparkConf, SparkFiles}
import water.support.{SparkContextSupport, SparkSessionSupport}


object DeepLearningDemoWithoutExtension extends SparkContextSupport with SparkSessionSupport {

  def main(args: Array[String]): Unit = {
    // Create a Spark config
    val conf: SparkConf = configure("Sparkling water: DL demo without Spark modification")

    // Create SparkContext to execute application on Spark cluster
    val sc = sparkContext(conf)
    addFiles(sc, absPath("examples/smalldata/allyears2k_headers.csv.gz"))

    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._

    //
    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val airlinesData = new H2OFrame(new File(SparkFiles.get("allyears2k_headers.csv.gz")))

    //
    // Use H2O to RDD transformation
    //
    import spark.implicits._
    val airlinesTable = h2oContext.asDataFrame(airlinesData)(sqlContext).map(row => AirlinesParse(row))
    println(s"\n===> Number of all flights via RDD#count call: ${airlinesTable.count()}\n")
    println(s"\n===> Number of all flights via H2O#Frame#count: ${airlinesData.numRows()}\n")

    //
    // Filter data with help of Spark SQL
    //
    airlinesTable.toDF.createOrReplaceTempView("airlinesTable")

    // Select only interesting columns and flights with destination in SFO
    val query = "SELECT * FROM airlinesTable WHERE Dest LIKE 'SFO'"
    val result: H2OFrame = sqlContext.sql(query) // Using a registered context and tables
    println(s"\n===> Number of flights with destination in SFO: ${result.numRows()}\n")

    //
    // Run Deep Learning
    //

    println("\n====> Running DeepLearning on the result of SQL query\n")
    // Result of SQL query
    val train = result('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
      'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
      'Distance, 'IsDepDelayed )
    train.replace(train.numCols()-1, train.lastVec().toCategoricalVec)
    train.update()

    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    // Use result of SQL query
    // Note: there is implicit conversion from RDD->H2OFrame->Key
    dlParams._train = train
    dlParams._response_column = 'IsDepDelayed

    //
    // Prepare Deep Learning parameters
    //
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    //
    // Use model for scoring
    //
    println("\n====> Making prediction with help of DeepLearning model\n")
    val predictionH2OFrame = dlModel.score(result)('predict)
    val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).take(10).map ( _.result.getOrElse("NaN") )
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    // Stop Spark cluster and destroy all executors
    if (System.getProperty("spark.ext.h2o.preserve.executors")==null) {
      sc.stop()
    }

    // Shutdown H2O
    h2oContext.stop()
  }
}
