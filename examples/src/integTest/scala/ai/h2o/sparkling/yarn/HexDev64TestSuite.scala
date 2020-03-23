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

package ai.h2o.sparkling.yarn

import ai.h2o.sparkling.YARNIntegrationTest
import org.apache.spark.h2o._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HexDev64TestSuite extends YARNIntegrationTest {

  ignore("HEX-DEV 64 test - airlines on big data") {
    launch(HexDev64Test)
  }
}

object HexDev64Test {

  def main(args: Array[String]): Unit = {
    val conf = H2OConf.checkSparkConf(new SparkConf().setAppName("HexDev64Test"))
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate()

    import h2oContext._
    // Import all year airlines into H2O
    val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
    val timer1 = new water.util.Timer
    val d = new java.net.URI(path)
    val airlinesData = new H2OFrame(d)
    val timeToParse = timer1.time / 1000
    println("Time it took to parse 116 million airlines = " + timeToParse + "secs")

    // Transfer data from H2O to Spark RDD
    val timer2 = new water.util.Timer
    implicit val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    val airlinesDataFrame = asDataFrame(airlinesData)
    val timeToTransfer = timer2.time / 1000
    println("Time it took to convert data to SparkRDD = " + timeToTransfer + "secs")

    assert(airlinesData.numRows == airlinesDataFrame.count, "Transfer of H2ORDD to SparkRDD completed!")

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
