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

import hex.kmeans.KMeansModel.KMeansParameters
import hex.kmeans.{KMeans, KMeansModel}
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.examples.h2o.DemoUtils.{configure,addFiles}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SQLContext
import water._
import water.fvec.DataFrame

/* Demonstrates:
   - data transfer from RDD into H2O
   - algorithm call
 */
object ProstateDemo {

  def main(args: Array[String]) {

    // Create Spark context which will drive computation
    val conf = configure("Sparkling Water: Prostate demo")
    val sc = new SparkContext(conf)
    // Add a file to be available for cluster mode
    addFiles(sc, "examples/smalldata/prostate.csv")

    // Run H2O cluster inside Spark cluster
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    // We do not need to wait for H2O cloud since it will be launched by backend

    // Load raw data
    val parse = ProstateParse
    val rawdata = sc.textFile(SparkFiles.get("prostate.csv"), 2)
    // Parse data into plain RDD[Prostate]
    val table = rawdata.map(_.split(",")).map(line => parse(line))

    // Convert to SQL type RDD
    val sqlContext = new SQLContext(sc)
    import sqlContext._ // import implicit conversions
    table.registerTempTable("prostate_table")

    // Invoke query on data; select a subsample
    val query = "SELECT * FROM prostate_table WHERE CAPSULE=1"
    val result = sql(query) // Using a registered context and tables

    // Build a KMeans model, setting model parameters via a Properties
    val model = runKmeans(result)
    println(model)

    // FIXME: shutdown H2O cloud not JVMs since we are embedded inside Spark JVM
    // Stop Spark local worker; stop H2O worker
    sc.stop()
  }

  private def runKmeans[T](trainDataFrame: DataFrame): KMeansModel = {
    val params = new KMeansParameters
    params._train = trainDataFrame._key
    params._K = 3
    // Create a builder
    val job = new KMeans(params)
    // Launch a job and wait for the end.
    val kmm = job.trainModel.get
    job.remove()
    // Print the JSON model
    println(new String(kmm._output.writeJSON(new AutoBuffer()).buf()))
    // Return a model
    kmm
  }
}

