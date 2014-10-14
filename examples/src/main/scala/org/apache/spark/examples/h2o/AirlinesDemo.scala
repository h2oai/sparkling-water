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
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.{H2ORDD, RDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import water.fvec.DataFrame

object AirlinesDemo {

  def main(args: Array[String]) {

    // Create Spark context which will drive computation
    // By default we use local spark context (which is useful for development)
    // but for cluster spark context, you should pass
    // VM option -Dspark.master=spark://localhost:7077
    val sc = createSparkContext()
    val sqlContext = new SQLContext(sc)
    import sqlContext._ // import implicit conversions

    // Start H2O-in-Spark
    if (sc.conf.get("spark.master").startsWith("local")) {
      water.H2OApp.main2("../h2o-dev")
      water.H2O.waitForCloudSize(1 /*One H2ONode to match the one Spark local-mode worker*/ , 1000)
    }

    // Load data into H2O
    val hc = new H2OContext(sc)
    val airlines = new DataFrame(new File("h2o-examples/smalldata/allyears2k_headers.csv.gz"))
    //val h2ordd = hc.parse[Airlines]("airlines.hex",
    //println(h2ordd.take(1)(0))

    //// Load raw data
    //val parse = AirlinesParse
    //val rawdata = sc.textFile("h2o-examples/smalldata/allyears2k_headers.csv.gz",2)
    //// Drop the header line
    //val noheaderdata =
    //    rawdata.mapPartitionsWithIndex((partitionIdx: Int, lines: Iterator[String]) => {
    //  if (partitionIdx == 0) lines.drop(1)
    //  lines
    //})
    //
    //// Parse raw data per line and produce RDD of Airlines objects
    //val table : RDD[Airlines] = noheaderdata.map(_.split(",")).map(line => parse(line))
    //table.registerTempTable("airlines_table")
    //println(table.take(2).map(_.toString).mkString("\n"))
    //
    //// Invoke query on a sample of data
    ////val query = "SELECT * FROM airlines_table WHERE capsule=1"
    ////val result = sql(query) // Using a registered context and tables
    //
    //// Map data into H2O frame and run an algorithm
    //
    //// Register RDD as a frame which will cause data transfer
    ////  - Invoked on result of SQL query, hence SQLSchema is used
    ////  - This needs RDD -> H2ORDD implicit conversion, H2ORDDLike contains registerFrame
    //// This will not work so far:
    //val h2oFrame = hc.createH2ORDD(table, "airlines.hex")

    // Build a KMeansV2 model, setting model parameters via a Properties
    //val props = new Properties
    //for ((k,v) <- Seq("K"->"3")) props.setProperty(k,v)
    //val job = new KMeansV2().fillFromParms(props).createImpl(h2ordd.fr)
    //val kmm = job.train().get()
    //job.remove()
    //// Print the JSON model
    //println(new String(kmm._output.writeJSON(new AutoBuffer()).buf()))

    // Stop Spark local worker; stop H2O worker
    sc.stop()
    water.H2O.exit(0)
  }

  private def createSparkContext(sparkMaster:String = null): SparkContext = {
    // Create application configuration
    val conf = new SparkConf()
      .setAppName("H2O Integration Example")
      //.set("spark.executor.memory", "1g")
    //if (!local)
    //  conf.setJars(Seq("h2o-examples/target/spark-h2o-examples_2.10-1.1.0-SNAPSHOT.jar"))
    if (System.getProperty("spark.master")==null) conf.setMaster("local")
    new SparkContext(conf)
  }
}

