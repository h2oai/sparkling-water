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

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.examples.utils.LocalIntegrationTest
import hex.kmeans.KMeansModel.KMeansParameters
import org.apache.spark.h2o._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.util.Timer

@RunWith(classOf[JUnitRunner])
class KMeansITestSuite extends LocalIntegrationTest {

  ignore("MLlib KMeans on airlines_all data") {
    launch(KMeansITest)
  }
}

/**
 * Test runner loading large airlines data from YARN HDFS via H2O API
 * transforming them into RDD and launching MLlib K-means.
 */
object KMeansITest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansITest")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate()
    import h2oContext._

    // Import all year airlines into H2O
    val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
    val timer1 = new water.util.Timer
    val d = new java.net.URI(path)
    val airlinesData = new H2OFrame(d)
    val timeToParse = timer1.time

    // Run Kmeans in H2O
    val ignore_columns = airlinesData.names diff Array("Month", "DayofMonth", "DayOfWeek")
    val H2OKMTimer = new Timer
    val kmeansParams = new KMeansParameters()
    kmeansParams._k = 5
    kmeansParams._max_iterations = 10
    kmeansParams._train = airlinesData
    kmeansParams._ignored_columns = ignore_columns
    kmeansParams._standardize = false
    val KmeansModel = new hex.kmeans.KMeans(kmeansParams).trainModel().get()
    val H2OKMBuildTime = H2OKMTimer.time

    // Score in H2O
    implicit val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    import sqlContext.implicits._
    val pred = KmeansModel.score(airlinesData)
    val predDF = asDataFrame(pred)
    val clusterCounts = predDF.rdd.countByValue()

    // Run Kmeans in Spark
    val sqlQueryTimer = new water.util.Timer
    val airlinesDF = asDataFrame(airlinesData)
    airlinesDF.createOrReplaceTempView("airlinesRDD")
    val airlinesTable = sqlContext.sql(
      """SELECT Month, DayofMonth, DayOfWeek FROM airlinesRDD"""
    )
    val sqlQueryTime = sqlQueryTimer.time

    assert(airlinesData.numRows == airlinesTable.count, "Transfer of H2ORDD to SparkRDD completed!")

    // Run Kmeans in Spark  on indices 10,19,26 (FlightNo, Distance, WeatherDelay)

    val airlinesVectorRDD = airlinesTable.rdd.map(row => Vectors.dense(row.getByte(0) * 1.0, row.getByte(1) * 1.0, row.getByte(2) * 1.0))
    val SparkKMTimer = new water.util.Timer
    val clusters = KMeans.train(airlinesVectorRDD, 5, 10)
    val SparkKMBuildTime = SparkKMTimer.time

    // Predict on Spark's Kmeans
    val sparkPredRDD = clusters.predict(airlinesVectorRDD)
    val srdd: DataFrame = sparkPredRDD.map(v => IntHolder(Option(v))).toDF
    val df: H2OFrame = srdd
    val sparkClusterCounts = sparkPredRDD.countByValue()

    // Get Within Set Sum of Squared Errors
    val sparkWSSSE = clusters.computeCost(airlinesVectorRDD)
    val h2oWSSSE = KmeansModel._output._withinss.fold(0.0)(_ + _)

    println("Spark: Within Set Sum of Squared Errors = " + sparkWSSSE)
    println("Spark: Time to Build (s) = " + SparkKMBuildTime)
    println("H2O: Within Set Sum of Squared Errors = " + h2oWSSSE)
    println("H2O: Time to Build (s) = " + H2OKMBuildTime)

    val relativeMeanDiff = (sparkWSSSE - h2oWSSSE) / sparkWSSSE
    assert(relativeMeanDiff < 0.1, "Within Set Sum of Squared Errors matches!")

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
