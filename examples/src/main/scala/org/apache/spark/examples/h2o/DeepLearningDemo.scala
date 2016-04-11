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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkFiles}
import water.app.SparkContextSupport


object DeepLearningDemo extends SparkContextSupport {

  def main(args: Array[String]): Unit = {
    // Create Spark context which will drive computation.
    val conf = configure("Sparkling Water: Deep Learning on Airlines data")
    val sc = new SparkContext(conf)
    addFiles(sc, absPath("examples/smalldata/allyears2k_headers.csv.gz"))

    // Run H2O cluster inside Spark cluster
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
    val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
    println(s"\n===> Number of all flights via RDD#count call: ${airlinesTable.count()}\n")
    println(s"\n===> Number of all flights via H2O#Frame#count: ${airlinesData.numRows()}\n")

    //
    // Filter data with help of Spark SQL
    //

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._ // import implicit conversions
    airlinesTable.toDF.registerTempTable("airlinesTable")

    // Select only interesting columns and flights with destination in SFO
    val query = "SELECT * FROM airlinesTable WHERE Dest LIKE 'SFO'"
    val result : H2OFrame = sqlContext.sql(query) // Using a registered context and tables
    println(s"\n===> Number of flights with destination in SFO: ${result.numRows()}\n")

    //
    // Run Deep Learning
    //

    println("\n====> Running DeepLearning on the result of SQL query\n")
    // Training data
    val train = result('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
      'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
      'Distance, 'IsDepDelayed )
    train.replace(train.numCols()-1, train.lastVec().toCategoricalVec)
    train.update()

    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._response_column = 'IsDepDelayed

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    //
    // Use model for scoring
    //
    println("\n====> Making prediction with help of DeepLearning model\n")
    val predictionH2OFrame = dlModel.score(result)('predict)
    val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map ( _.result.getOrElse("NaN") )
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    // Stop Spark cluster and destroy all executors
    if (System.getProperty("spark.ext.h2o.preserve.executors")==null) {
      sc.stop()
    }
    // Shutdown H2O
    h2oContext.stop()
  }

}
